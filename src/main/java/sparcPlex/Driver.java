/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex;
 
import cplexLib.dataTypes.*;

import org.apache.log4j.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;

import static cplexLib.constantsAndParams.Constants.*;
import static cplexLib.constantsAndParams.Parameters.*; 
import cplexLib.dataTypes.ActiveSubtreeCollection;

import java.time.*;
import java.util.*; 
import java.util.Map.Entry;

import scala.Tuple2; 
import cplexLib.dataTypes.SolutionComparator; 
import static sparcPlex.constantsAndParams.Constants.*;
import static sparcPlex.constantsAndParams.Parameters.*;
import sparcPlex.functions.AttachmentConverter;
import sparcPlex.functions.CplexBasedSolver;
import sparcPlex.functions.NodeMerger;
import sparcPlex.functions.NodePlucker;
import sparcPlex.functions.PartitionKeyAdder;
import sparcPlex.functions.PendingNodeMetaDataFetcher;
import sparcPlex.functions.TreeCounter;
import sparcPlex.intermediateDataTypes.SolverResult;
import sparcPlex.loadbalancing.AveragingHeuristic;

/**
 *
 * @author srini
 * 
 * Driver for the SparcPlex framework
 * Use the CPlex library to distribute a BnB computation over Spark
 * 
 * 
 */
public class Driver {
    
    private static Logger logger=Logger.getLogger(Driver.class);
    
    
    public static void main(String[] args) throws Exception   {

        logger.setLevel(Level.DEBUG);
        PatternLayout layout =new PatternLayout("%5p  %d  %F  %L  %m%n");        
        logger.addAppender(new FileAppender(layout, DRIVER_LOG_FILE));

        //Driver for distributing the CPLEX  BnB solver on Spark
        SparkConf conf = new SparkConf().setAppName("SparcPlex V4");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //We have an RDD which holds the frontier, one collection per partition
        //the key is the partition number
        //We first create an empty frontier, and then add the original problem to the collection on partition 1
        JavaPairRDD < Integer, ActiveSubtreeCollection> frontier ; 
                
        //Initially the frontier only has the original problem  
        // Original problem is represented by an empty node attachment, i.e. no branching variables  
        //
        //let us start this   node on partition 1, partition 0 is the driver
        //
        // Note how the RDD is created, we never shuffle ActiveSubTreeCollection objects ; Only node attachments are shuffled across the network
        
        List<Tuple2<Integer, List<NodeAttachment>>> initialList  = new ArrayList<Tuple2<Integer, List<NodeAttachment>>>  ();
        for (int index = ONE; index <= NUM_PARTITIONS; index ++){
            List<NodeAttachment> attachmentList = new ArrayList<NodeAttachment>();
            if (index == ONE) attachmentList.add(new NodeAttachment()) ;
            initialList.add(new Tuple2<Integer, List<NodeAttachment>> (index, attachmentList));
        }
        
        frontier =  sc.parallelize(initialList) 
                 /* add the key # of the partition we want to place it on*/
                .mapToPair( new PartitionKeyAdder ())  
                /*realize the movement to desired partition*/
                .partitionBy(new HashPartitioner(NUM_PARTITIONS)) 
                /* finally convert the attachment list into an ActiveSubTreeCollection object*/
                .mapValues( new AttachmentConverter() ); 
        
        //Frontier is used many times, so cache it.
        frontier.cache();
        
        
        //For each partition , we maintain a list of pending nodes in each subtree on that partition
        //Recall that each NodeAttachmentMetadata has the subtree ID, and details about the node
        Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap = new HashMap <Integer,List<NodeAttachmentMetadata> > ();
        //go out to the cluster and find now many unsolved kids each partition has
        JavaPairRDD < Integer, List<NodeAttachmentMetadata>> perPartitionPendingNodesRDD = frontier.mapPartitionsToPair(new PendingNodeMetaDataFetcher(), true);
        //no need to cache the RDD as it is used only once
        perPartitionPendingNodesMap= perPartitionPendingNodesRDD.collectAsMap();
        //At this point, we should have empty lists in the perPartitionPendingNodesMap for all partitions except partition 1, which should have the original problem
                       
        //initialize  the incumbent 
        Solution incumbent = new Solution( ); 
        //solution is halted at once , in case some node is found to be unbounded or in error
        boolean isSolutionHalted=   false;
        
        //loop till frontier is empty, or some iteration count or time limit is exceeded
        int iteration = ZERO;
        for (;iteration <MAX_SPARK_ITERATIONS;iteration++){
                        
            //STEP 0 : 
            //*****************************************************************************************************************
            //Prepare for this iteration, if it is needed at all.
            
            //count the number of trees left to solve in the frontier
            long treecount =ZERO;
            Map<Integer, Integer> treeCountPerPartitionMap =frontier.mapToPair(new TreeCounter()).collectAsMap();
            for (Integer count : treeCountPerPartitionMap.values()){
                treecount +=count;
            }
              
            if ( treecount  == ZERO) {                
                //we are done, no active subtrees left to solve
                logger.debug("All trees solved." );
                break;                 
            } else {
                logger.debug("Starting iteration " + iteration+", with "+treecount +" trees.") ;
            }
            
            //Use the perPartitionPendingNodesMap and treeCountPerPartitionMap to decide cycle iteration time
            //Every partition must stop on or before this clock time
            Instant endTimeOnWorkerMachines = getIterationEndTime( treeCountPerPartitionMap, perPartitionPendingNodesMap);
            
            //STEP 1 : 
            //*****************************************************************************************************************
            //Solve for some time. From each partition, we get the the best solution and pending kids.
            //Note that child nodes are represented by their meta-data, which contains node ID and the containing subtree's unique ID
            //Also note that solving a collection cleans up any trees in it that have been completely solved, or discarded
            CplexBasedSolver cplexSolver = new CplexBasedSolver(   endTimeOnWorkerMachines, incumbent ,  iteration);
            JavaPairRDD< Integer, SolverResult> resultsRDD  = frontier.mapPartitionsToPair( cplexSolver, true);
            //RDD is used more than once, so cache it
            resultsRDD.cache(); 
            
            
            //STEP 2 :  
            //*****************************************************************************************************************
            //use the solutions found in this iteration to update the incumbent
            Map< Integer, SolverResult> resultsMap= resultsRDD.collectAsMap();
            Collection<SolverResult> solverResultCollection = resultsMap.values();
            
            for (SolverResult solverResult:solverResultCollection){     
                
                Solution solutionFromPartition =  solverResult.getSolution();
                
                if (  solutionFromPartition.isUnbounded()) {
                     logger.info("Solution is unbounded, will exit.");
                     isSolutionHalted= true;
                     //erroneous or unbounded solution overrides any existing solution
                     incumbent = solutionFromPartition;     
                     break;
                }
                if (  solutionFromPartition.isError()) {
                     logger.error("Solution is in error, will exit.");
                     isSolutionHalted= true;
                     incumbent = solutionFromPartition;     
                     break;
                }                
                if ( ZERO != (new SolutionComparator()).compare(incumbent, solutionFromPartition)){
                    //we have found a better solution
                    incumbent = solutionFromPartition;        
                }
            }
            
            if (   isSolutionHalted) break;
           
            
            //STEP 3 : 
            //*****************************************************************************************************************
            // use the solutions found in this iteration to update the perPartitionPendingNodesMap  
            
            for (Entry<Integer, SolverResult> entry   :resultsMap.entrySet()){
                perPartitionPendingNodesMap.put ( entry.getKey(), entry.getValue().getNodeList() );
            }
            
            
            //STEP 4 :
            //*****************************************************************************************************************
            //If perPartitionPendingNodesMap is empty , no node migration needs to be done.
            //Otherwise we must load balance, and update perPartitionPendingNodesMap .
            if (! perPartitionPendingNodesMap.isEmpty() ){
                 

                //STEP 4a : 
                //*****************************************************************************************************************
                //use perPartitionPendingNodesMap to decide how to move nodes
                AveragingHeuristic loabBalancingHueristic = new AveragingHeuristic(perPartitionPendingNodesMap);
                Map <Integer, List<String >> nodesToPluckOut = loabBalancingHueristic.nodesToPluckOut;
                Map <Integer, Integer> nodesToMoveIn =loabBalancingHueristic.nodesToMoveIn;



                //STEP 4b : 
                //*****************************************************************************************************************            
                //farm out nodes. This time we fetch the whole node attachment , not just meta-data about the node.
                //Key is the partition id
                Map <Integer, List<NodeAttachment > > farmedNodes = new HashMap <Integer,List<NodeAttachment > > ();
                JavaPairRDD<Integer, List<NodeAttachment >> farmedNodesRDD = frontier.mapPartitionsToPair(new NodePlucker (  nodesToPluckOut) , true);
                farmedNodes = farmedNodesRDD.collectAsMap();
                
                //remove entries from map farmedNodes, if list of nodes farmed from the partition is empty
                farmedNodes = removeEmptyLists(farmedNodes);

                //STEP 4c : 
                //*****************************************************************************************************************            
                //Prepare to migrate farmed out nodes to their new home.
                //The data structure 'nodesToMoveIn' tells us how many to move to each destination partition
                // The data structure migratedNodesList tells use which farmed node is heading where
                List<Tuple2<Integer,NodeAttachment>> migratedNodesList = new ArrayList<Tuple2<Integer,NodeAttachment>> ();

                for (Entry<Integer, Integer> entry : nodesToMoveIn.entrySet()){
                    int partitionID = entry.getKey();
                    int count = entry.getValue();

                    //get count nodes from farmedNodes, and move them into partition partitionID
                    migratedNodesList .addAll( getSubSetOfFarmedNodes(farmedNodes, count, partitionID)) ;                
                }
                  

                //STEP 4d : 
                //*****************************************************************************************************************
                //merge migrated nodes with existing frontier
                frontier = frontier.mapPartitionsToPair(new NodeMerger(migratedNodesList));
                //cache the new frontier
                frontier.cache();
                
                
                //STEP 4e : 
                //*****************************************************************************************************************
                // update perPartitionPendingNodesMap by recounting number of kids pending on each tree.
                
                perPartitionPendingNodesRDD = frontier.mapPartitionsToPair(new PendingNodeMetaDataFetcher(), true);
                //no need to cache the RDD as it is used only once
                perPartitionPendingNodesMap= perPartitionPendingNodesRDD.collectAsMap();
                
            } //end if perPartitionPendingNodesMap is not empty
                    
                   
            //do the next iteration
                    
            
        } //end driver iterations
        
        //
        logger.info("Sparcplex V4 completed in "+  iteration +" iterations." ) ;
        
        String status =  INFEASIBLE_SOLUTION   ;
        if (incumbent.isFeasible()) status = FEASIBLE_SOLUTION;
        if (incumbent.isOptimal()) status = OPTIMAL_SOLUTION;            
        if (incumbent.isError()) status = ERROR_SOLUTION;       
        if (incumbent.isUnbounded()) status = UNBOUNDED_SOLUTION;       
        logger.info("Solution status is " + status); 
        
        if (isSolutionHalted) {
             logger.info("Solution was halted "  ); 
        } else {
            if (incumbent.isFeasibleOrOptimal()) {
                logger.info( incumbent ); 
            } 
        }
        
         
    }//end main
    
    private static Instant getIterationEndTime ( 
            Map<Integer, Integer> treeCountPerPartitionMap,
            Map <Integer, List<NodeAttachmentMetadata> > perPartitionPendingNodesMap) {   
        double timeSlice = ZERO;
       
        
        
        //calculate the time slice using node meta data of each partition 
        timeSlice = ITERATION_TIME_MAX_SECONDS;
            
        return      Instant.now().plusMillis(THOUSAND*(int)timeSlice );
        
    } 
    
    //extract count nodes from available farmed out nodes
    private static  List<Tuple2<Integer,NodeAttachment>>  getSubSetOfFarmedNodes(Map <Integer, List<NodeAttachment > >  farmedNodes, int count, int partitionID ) {
        List<Tuple2<Integer,NodeAttachment>> retval = new ArrayList<Tuple2<Integer,NodeAttachment>> ();
        
        while (count > ZERO) {
            NodeAttachment node =removeOneNodeFromMap(farmedNodes);
            Tuple2<Integer,NodeAttachment> tuple = new Tuple2<Integer,NodeAttachment>(partitionID, node);
            retval.add(tuple);
            
            count = count - ONE;
        }
        
        return retval;
    }
    
    private static NodeAttachment removeOneNodeFromMap (Map <Integer, List<NodeAttachment > >  farmedNodes) {
        NodeAttachment node =null;
        
        List<NodeAttachment > nodeList = null;
        int pid = ZERO;
        
        for (Entry <Integer, List<NodeAttachment >> entry :farmedNodes.entrySet() ) {
            nodeList= entry.getValue();
            node=nodeList.remove(ZERO);      
            pid = entry.getKey();
            break;
        }
        if (nodeList.size()>ZERO) {
            farmedNodes.put(pid, nodeList);
        }else {
            farmedNodes.remove(pid);
        }
        
        return node;
    }
    
    private static Map <Integer, List<NodeAttachment > > removeEmptyLists(Map <Integer, List<NodeAttachment > > farmedNodes){
        Map <Integer, List<NodeAttachment > > filteredFarmedNodes = new HashMap <Integer,List<NodeAttachment > > ();
        for(Entry <Integer, List<NodeAttachment > > entry :farmedNodes.entrySet()){
            int key = entry.getKey();
            List<NodeAttachment >  value = entry.getValue();
            if(value !=null && value.size()>ZERO) filteredFarmedNodes.put(key, value);
        }
        return filteredFarmedNodes;
    }
    
}//end driver class

