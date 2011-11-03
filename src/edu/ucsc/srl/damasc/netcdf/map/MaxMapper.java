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
import edu.ucsc.srl.damasc.netcdf.io.GroupIDGen;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * A Mapper class that applies the Max function
 */
public class MaxMapper extends Mapper<ArraySpec, Array, GroupID, IntWritable> {

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

    int[] allOnes = new int[key.getShape().length];
    for( int i=0; i<allOnes.length; i++) {
      allOnes[i] = 1;
    }

    // create a hash with the key being the group id and the 
    // value being max seen so far

    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 key.getShape().length);

    HashMap<GroupID, Array> groupSubArrayMap = new HashMap<GroupID, Array>();

    GroupIDGen myGIDG = new GroupIDGen();
    GroupIDGen.pullOutSubArrays( myGIDG, ncArray, key, extractionShape, 
                                 allOnes, groupSubArrayMap);

    ArrayInt localArray;
    GroupID localGID;
    // now roll through all the entries in the HashMap

    int invalidMapperCounter = 0;
    int currentMax = Integer.MIN_VALUE;
    int tempValue = Integer.MIN_VALUE;

    IntWritable myIntW = new IntWritable();
    LongWritable myLongW = new LongWritable();

    Iterator<Map.Entry<GroupID, Array>> gidItr = 
          groupSubArrayMap.entrySet().iterator();

    while (gidItr.hasNext() ) { 
      currentMax = Integer.MIN_VALUE;
      Map.Entry<GroupID, Array> pairs = gidItr.next();
      localGID = pairs.getKey();
      localArray = (ArrayInt)pairs.getValue();

      // TODO sort out how to do filtering with this new GroupID based setup
      // -jbuck

      IndexIterator valItr = localArray.getIndexIterator();

      while( valItr.hasNext() ) {

        tempValue = valItr.getIntNext();
        if ( tempValue > currentMax ) {
          currentMax = tempValue;
        }
      }

      // write out the current groupID and the max value found for it
      //Utils.flatten(localGID);
      myIntW.set(currentMax);
      context.write(localGID, myIntW);

    }

    timerA = System.currentTimeMillis() - timerA;
    LOG.info("for corner " + Utils.arrayToString(key.getCorner()) + 
             " map loop time: " + 
             timerA + " ms with " );
  }
}
