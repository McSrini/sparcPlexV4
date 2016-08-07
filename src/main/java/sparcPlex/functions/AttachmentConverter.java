package  sparcPlex.functions;

import cplexLib.dataTypes.ActiveSubtreeCollection;
import cplexLib.dataTypes.NodeAttachment;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.function.Function; 

public class AttachmentConverter implements Function <List<NodeAttachment>, ActiveSubtreeCollection>, Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
 
    public ActiveSubtreeCollection call(List<NodeAttachment> nodeList) throws Exception {
         return new ActiveSubtreeCollection(nodeList);
    }
 

}
