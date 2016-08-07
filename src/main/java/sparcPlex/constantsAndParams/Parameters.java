/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparcPlex.constantsAndParams;

import static cplexLib.constantsAndParams.Constants.*;
import java.io.Serializable;

/**
 *
 * @author srini
 */
public class Parameters implements Serializable{
    
    public  static final  int CORES_PER_MACHINE = 4;
    public  static final  int NUM_MACHINES= TEN;
    
    //each core = 1 partition
    public  static final  int NUM_PARTITIONS = -ONE + CORES_PER_MACHINE * NUM_MACHINES ; //  1 core will be taken up by the driver
      
    public static final int MAX_SPARK_ITERATIONS=THOUSAND;
      
   
    public  static final  double   ITERATION_TIME_MIN_SECONDS =   3*60 ;  //3 minutes
     public  static final  double   ITERATION_TIME_MAX_SECONDS =    10*ITERATION_TIME_MIN_SECONDS ; 
    public  static final  double   THRESHOLD_OF_HARD_LEAF_COUNT_PER_PARTITION =   10 ;  //if fewer, then use ITERATION_TIME_MIN_SECONDS
        
    
}
