package sparcPlex.functions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.*;

import scala.Tuple2;
import cplexLib.dataTypes.ActiveSubtreeCollection;
import cplexLib.dataTypes.NodeAttachmentMetadata;

public class TreeCounter implements  PairFunction< Tuple2<Integer,ActiveSubtreeCollection> , Integer , Integer> ,Serializable{
 
    public Tuple2<Integer, Integer> call(
            Tuple2<Integer, ActiveSubtreeCollection> inputTuple) throws Exception {
         int partitionID = inputTuple._1 ;
         ActiveSubtreeCollection collection = inputTuple._2;
         return new Tuple2<Integer, Integer> ( partitionID, collection.getNumberOFTrees());
    }
 
    

}
