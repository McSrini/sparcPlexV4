package sparcPlex.functions;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

import static cplexLib.constantsAndParams.Constants.*;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import cplexLib.dataTypes.*;
import scala.Tuple2;
import sparcPlex.intermediateDataTypes.SolverResult;

public class CplexBasedSolver implements PairFlatMapFunction<Iterator<Tuple2<Integer,ActiveSubtreeCollection>>, Integer, SolverResult> , Serializable{
    
    private final Solution bestKnownGlobalSolution;
    private final int driverIteration;
    
    private final Instant endTimeOnWorkerMachine;
    
    public CplexBasedSolver (Instant endTimeOnWorkerMachine,        Solution bestKnownGlobalSolution , int driverIteration  ){
        
        this.endTimeOnWorkerMachine=endTimeOnWorkerMachine;        
        this.bestKnownGlobalSolution = bestKnownGlobalSolution ;        
        this.driverIteration=driverIteration;
       
    }
 
    public Iterable<Tuple2<Integer, SolverResult>> call(
            Iterator<Tuple2<Integer, ActiveSubtreeCollection>> inputIterator)
            throws Exception {
        
        //our return value
        List <Tuple2<Integer, SolverResult>> retval = new ArrayList<Tuple2<Integer, SolverResult>>();
        
        Tuple2<Integer, ActiveSubtreeCollection> inputTuple = null;
        int partitionId = ZERO;
        
        //we only have 1 collection per partition, but we can loop thru our "list" of collections
        while ( inputIterator.hasNext()){
            
            inputTuple = inputIterator.next();
            partitionId = inputTuple._1;
            ActiveSubtreeCollection subTreeCollection = inputTuple._2;
            
            //solve the collection for specified amount of time
            Solution soln = subTreeCollection.solve(endTimeOnWorkerMachine , bestKnownGlobalSolution, driverIteration, partitionId );
            //remove completed trees from partition
            subTreeCollection.cleanCompletedAndDiscardedTrees();
            //collect the pending child nodes
            List<NodeAttachmentMetadata> pendingChildNodeList = subTreeCollection.getNodeMetaData();
            
            //prepare our return value
            SolverResult solverResult= new SolverResult (  soln, pendingChildNodeList);
            retval.add(new Tuple2<Integer, SolverResult>(partitionId,solverResult));
        }
        
        return retval;
    }

}
