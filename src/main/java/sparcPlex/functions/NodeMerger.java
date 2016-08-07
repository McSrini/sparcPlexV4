package sparcPlex.functions;

import java.util.Iterator;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import cplexLib.dataTypes.ActiveSubtreeCollection;
import cplexLib.dataTypes.NodeAttachment;
import cplexLib.dataTypes.NodeAttachmentMetadata;

public class NodeMerger implements PairFlatMapFunction<
                                         Iterator<Tuple2< Integer, ActiveSubtreeCollection>>, 
                                         Integer, ActiveSubtreeCollection 
                                         >{
 
  
    private List<Tuple2<Integer,NodeAttachment>> newPairs ;
    
    public NodeMerger (List<Tuple2<Integer,NodeAttachment>> newPairs) {
        this.newPairs=newPairs;
    }
    
    public Iterable<Tuple2<Integer, ActiveSubtreeCollection>> call(
            Iterator<Tuple2<Integer, ActiveSubtreeCollection>> iterator)
            throws Exception {
        
        List<Tuple2<Integer, ActiveSubtreeCollection>> mergedList = new ArrayList<Tuple2<Integer, ActiveSubtreeCollection>>();
         
        //only 1 collection per partition, so the while loop will execute only once
        while ( iterator.hasNext()){
            
            Tuple2<Integer, ActiveSubtreeCollection>  inputTuple = iterator.next();
            int partitionId = inputTuple._1;
            ActiveSubtreeCollection collection = inputTuple._2;
            
            //find the nodes to be added as new trees into this collection
            //if no additions, just output the collection as it is
            collection.add( getNodeList( partitionId) );
            Tuple2<Integer, ActiveSubtreeCollection> newTuple = new Tuple2<Integer, ActiveSubtreeCollection> (partitionId, collection   );
            mergedList.add(newTuple);
                       
        }
        return mergedList;
    }
 

    private List<NodeAttachment> getNodeList(int partitionID) {
        List<NodeAttachment> nodeList = new ArrayList<NodeAttachment> ();
        for(Tuple2<Integer,NodeAttachment> tuple: newPairs){
            if(tuple._1==partitionID) nodeList.add(tuple._2);
        }
        return nodeList;
    }
 

}
