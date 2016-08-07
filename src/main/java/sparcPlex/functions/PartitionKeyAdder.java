package  sparcPlex.functions;

import cplexLib.dataTypes.NodeAttachment;
import java.io.Serializable;
import java.util.*;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;   

/**
 * 
 * @author SRINI
 *
 * add a key to convert list into pair
 */
public class PartitionKeyAdder implements PairFunction<Tuple2<Integer, List<NodeAttachment>>,Integer,List<NodeAttachment>> ,Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = -7474219718414305808L;
    
    public Tuple2<Integer, List<NodeAttachment>> call(Tuple2<Integer, List<NodeAttachment>> inputTuple) throws Exception {
        Tuple2<Integer, List<NodeAttachment>>  tuple = new Tuple2<Integer, List<NodeAttachment>> (inputTuple._1, inputTuple._2);

        return tuple ;
    }

     
}
