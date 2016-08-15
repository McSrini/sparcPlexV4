/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.functions;

import cplexLib.dataTypes.ActiveSubtreeCollection;
import java.util.*;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author srini
 */
public class InferiorTreeFilter implements  Function<ActiveSubtreeCollection,List<String>>{
    
    private double bestKnownOptimumValue;
    
    public InferiorTreeFilter (double bestKnownOptimumValue) {
        this.bestKnownOptimumValue = bestKnownOptimumValue;
    }
 
    //filter out all trees which are inferior to bestKnownOptimumValue
    public List<String> call(ActiveSubtreeCollection activeSubtreeCollection) throws Exception {
         
         return activeSubtreeCollection.removeInferiorTrees(bestKnownOptimumValue);
    }
        
    
}
