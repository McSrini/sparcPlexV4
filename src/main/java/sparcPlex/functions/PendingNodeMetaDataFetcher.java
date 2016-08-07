/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.functions;

import static cplexLib.constantsAndParams.Constants.*;
import cplexLib.dataTypes.*;
 
  
import java.io.Serializable; 
import java.util.Iterator;
import java.util.*;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author srini
 * 
 * from each partition, fetch list of unsolved child nodes  
 */
public class PendingNodeMetaDataFetcher implements  PairFlatMapFunction<Iterator<Tuple2<Integer,ActiveSubtreeCollection>>, Integer, List<NodeAttachmentMetadata>> ,Serializable{
 
    public Iterable<Tuple2<Integer, List<NodeAttachmentMetadata>>> call(Iterator<Tuple2<Integer, ActiveSubtreeCollection>> iterator) throws Exception {
        
        //our return value
        List<Tuple2<Integer, List<NodeAttachmentMetadata>>> resultList = new ArrayList<Tuple2<Integer, List<NodeAttachmentMetadata>>>();
                 
        Tuple2<Integer, ActiveSubtreeCollection> inputTuple = null;
        int partitionId = ZERO;
        
        //list of all node attachments on this partition
        List<NodeAttachmentMetadata> attachmentList = new ArrayList<NodeAttachmentMetadata>();
        
        //only 1 collection per partition, so the while loop will execute only once
        while ( iterator.hasNext()){
            
            inputTuple = iterator.next();
            partitionId = inputTuple._1;
            ActiveSubtreeCollection collection = inputTuple._2;
            
            attachmentList=collection. getNodeMetaData();
                       
        }
        
        Tuple2<Integer, List<NodeAttachmentMetadata>> resultTuple = new Tuple2<Integer, List<NodeAttachmentMetadata>>(partitionId, attachmentList);
        resultList.add(resultTuple);
        return resultList;
    }
    
}
