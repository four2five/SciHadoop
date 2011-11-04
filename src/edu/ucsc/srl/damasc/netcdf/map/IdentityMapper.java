package edu.ucsc.srl.damasc.netcdf.map;

import java.io.IOException;

import java.lang.Integer;
import java.lang.Long;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.IndexIterator;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * Mapper for the Identity function
 * Currently uses an in-memory combiner
 */
public class IdentityMapper extends Mapper<ArraySpec, Array, LongWritable, IntWritable> {

  private static final Log LOG = LogFactory.getLog(IdentityMapper.class);
  private static boolean _benchmarkArraySpec = true;

  public static enum InvalidCell { INVALID_CELL_COUNT } ;

  /**
   * Reduces values for a given key
   * @param key ArraySpec representing the given Array being passed in
   * @param value an Array to process that corresponds to the given key 
   * @param context the Context object for the currently executing job
   */
  public void map(ArraySpec key, Array value, Context context)
                  throws IOException, InterruptedException {

    long timerA = System.currentTimeMillis();

    ArrayInt ncArray = (ArrayInt)value;
    int numCells = (int)ncArray.getSize();

    // this is a fair bit of memeory. Better to do it in one allocation, than a lot of them,
    // but we should still try to optimize this out

    int[] globalCoord = new int[key.getShape().length];
    int[] groupIDArray = new int[key.getShape().length];

    int[] allOnes = new int[key.getShape().length];
    for( int i=0; i<allOnes.length; i++) {
      allOnes[i] = 1;
    }

    // create a hash with the key being the group id and the 
    // value being the associated value
    HashMap<Long, Integer> inMapperCombiner = new HashMap<Long, Integer>();

    long[] longArray = new long[numCells];
    int[] intArray = new int[numCells];

    IndexIterator iter = ncArray.getIndexIterator();

    GroupID myGroupID = new GroupID();
    myGroupID.setName(key.getVarName());

    IntWritable myIntW = new IntWritable();
    LongWritable myLongW = new LongWritable();

    //Integer myInt = new Integer();
    //Long myLong = new Long();

    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 key.getShape().length);
    int[] variableShape = 
        Utils.getVariableShape(context.getConfiguration());

    int invalidMapperCounter = 0;
    int val = 0;
    int counter = 0;
    long startup = System.currentTimeMillis() - timerA;
    LOG.info("Startup time: " + startup + " ms");

    timerA = System.currentTimeMillis();

    while( iter.hasNext() ) {

      val = iter.getIntNext();

      // orient this cell's location in the global space
      globalCoord = Utils.mapToGlobal(iter.getCurrentCounter(), 
                                key.getCorner(), globalCoord );

      // track cells that are invalid. This is typically due 
      // to filtering of cells by the query
      // it's only applied at the mapper for optB. 
      // optC applies it at split generation

      if ( !Utils.noScanEnabled(context.getConfiguration()) ) 
      { 
        if ( !Utils.isValid(globalCoord, context.getConfiguration()) ) {
          invalidMapperCounter++;
          continue;
        }
      }

      // figure out the "bucket" for this (determined by the 
      // extraction shape and position of this cell
      myGroupID = Utils.mapToLocal(globalCoord, groupIDArray, 
                                   myGroupID, extractionShape); 

      longArray[counter] = myGroupID.flatten(variableShape);
      intArray[counter] = val;

      if ( inMapperCombiner.containsKey(longArray[counter]) ){
        if ( intArray[counter] > inMapperCombiner.get(longArray[counter]) ){
          inMapperCombiner.put(longArray[counter], intArray[counter]);
        }
      } else {
        inMapperCombiner.put(longArray[counter], intArray[counter]);
      }
        
      counter++;
    }

    timerA = System.currentTimeMillis() - timerA;
    LOG.info("for corner " + Utils.arrayToString(key.getCorner()) + 
             " map loop time: " + 
             timerA + " ms with " + invalidMapperCounter + 
             " invalid ArraySpecs" );

    // stop the timer
    timerA = System.currentTimeMillis();

    // write out all results of the in-mapper combiner here
    Iterator<Map.Entry<Long, Integer>> itr = inMapperCombiner.entrySet().iterator();
    while(itr.hasNext()){
      Map.Entry<Long, Integer> pairs = itr.next();
      myLongW.set(pairs.getKey());
      myIntW.set(pairs.getValue() );
      context.write(myLongW, myIntW);
    }

    context.getCounter(InvalidCell.INVALID_CELL_COUNT).increment(invalidMapperCounter);
    timerA = System.currentTimeMillis() - timerA;
    LOG.info("writing data and increment counte took "  + 
             timerA + " ms" );
  }
}
