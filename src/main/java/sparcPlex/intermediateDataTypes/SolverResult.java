package sparcPlex.intermediateDataTypes;

import java.io.Serializable;
import java.util.*;
import static cplexLib.constantsAndParams.Constants.*;
import cplexLib.dataTypes.*;

/**
 * 
 * @author SRINI
 *
 *
 * Solver result is a list of child nodes pending solution, and a solution object.
 * In other words, it is the result of solving a set of subtrees (or an individual subtree).
 * 
 * Note that node list could be empty.
 * 
 * This object needs to be serializable. 
 * 
 */
public class SolverResult implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private Solution soln;
    private List<NodeAttachmentMetadata> attachmentList ;
    
    public SolverResult () {
        soln = new Solution();
        attachmentList = new ArrayList<NodeAttachmentMetadata>();
    }
    
    public SolverResult (Solution soln, List<NodeAttachmentMetadata> attachmentList) {
        this.soln = soln;
        this.attachmentList=attachmentList;
    }
    
    public Solution getSolution () {
        return soln;
    }

    public List<NodeAttachmentMetadata> getNodeList (){
        return attachmentList;
    }
    
    //We have been given a new solution, and nodes to be added
    public void merge (  Solution soln,      Collection<NodeAttachmentMetadata> attachmentList) {
        
        if ( ZERO != (new SolutionComparator()).compare(this.soln, soln)) 
            /* better solution has been found*/        
            this.soln = soln;
        
        this.attachmentList.addAll(attachmentList);
    }
   
}
