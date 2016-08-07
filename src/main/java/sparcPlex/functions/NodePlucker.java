/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.functions;

import static cplexLib.constantsAndParams.Constants.*;
import static cplexLib.constantsAndParams.Constants.ZERO;
import cplexLib.dataTypes.*;
import cplexLib.dataTypes.*;
import java.io.Serializable;
import java.util.*;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author srini
 */
public class NodePlucker implements  PairFlatMapFunction<Iterator<Tuple2<Integer,ActiveSubtreeCollection>>, Integer, List<NodeAttachment >> ,Serializable{
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private Map <Integer, List<String >> nodesToPluckOutMap;
            
    public NodePlucker ( Map <Integer, List<String >> nodesToPluckOutMap) {
        
        this.nodesToPluckOutMap = nodesToPluckOutMap;
        
    }
 
    public Iterable<Tuple2<Integer, List<NodeAttachment>>> call(Iterator<Tuple2<Integer, ActiveSubtreeCollection>> iterator) throws Exception {
        
        //our return value
        List<Tuple2<Integer, List<NodeAttachment >>> resultList = new ArrayList<Tuple2<Integer, List<NodeAttachment >>>();
                 
        Tuple2<Integer, ActiveSubtreeCollection> inputTuple = null;
        int partitionId = ZERO;
        
        //farmed nodes from this partition 
        List<NodeAttachment > farmedNodeList = new ArrayList<NodeAttachment > ();
        
        //process one subtree at a time 
        while ( iterator.hasNext()){
                        
            inputTuple = iterator.next();
            partitionId = inputTuple._1;
            ActiveSubtreeCollection collection = inputTuple._2;
            
            //find nodes to pluck from every tree in this collection
            for(String treeGuid : collection.getActiveSubtreeIDs()){
            
                List<String> nodesIDsToPluckFromTree =        
                        findNodeIDsToPluck(  treeGuid,   partitionId);
                                
                //remove these nodes from the tree and collect them
                for (String nodeID : nodesIDsToPluckFromTree ){
                    farmedNodeList.add(collection.farmOutNode ( treeGuid,  nodeID) );
                }
                
            }
             
            
        }
        
        Tuple2<Integer, List<NodeAttachment >> resultTuple = new Tuple2<Integer, List<NodeAttachment >>(partitionId, farmedNodeList);
        resultList.add(resultTuple);
        return resultList;
        
    }
    
    private List<String> findNodeIDsToPluck(String treeGUID, int partitionID){
        List<String> nodeIDList = new ArrayList<String>();
        List<String> treesAndNodes = nodesToPluckOutMap.get(partitionID);
        if(treesAndNodes !=null || treesAndNodes.size()>ZERO){
            //there are nodes to pluck from this partition
            for (String treeAndNode :treesAndNodes){
                String [] stringArray =treeAndNode.split(DELIMITER);
                String treeGuid = stringArray[ZERO];
                String nodeID= stringArray[ONE];
                if (treeGUID.equalsIgnoreCase(treeGuid)) nodeIDList.add(nodeID);
            }
        }
        return nodeIDList;
    }
          
}
