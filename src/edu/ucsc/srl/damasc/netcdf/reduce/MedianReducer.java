package edu.ucsc.srl.damasc.netcdf.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.HolisticResult;
import edu.ucsc.srl.damasc.netcdf.io.Result;

import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * Reducer for the Median operator
 */
public class MedianReducer extends 
             Reducer<GroupID, HolisticResult, GroupID, IntWritable> {

  private static final Log LOG = LogFactory.getLog(MedianReducer.class);

  static enum MedianReducerStatus { FULL, NOTFULL, NOTFINAL } 

  /**
   * Reduces all the values for the given key, produces the median of the
   * HolisticResult objects in values
   * @param key the flattened corner for this instance of the extraction shape 
   * in the global logical space
   * @param values an Iterable list of HolisticResult objects that represent all the inputs
   * for this key
   * @param context the Context object for the executing program
   */
  public void reduce(GroupID key, Iterable<HolisticResult> values, 
                     Context context)
                     throws IOException, InterruptedException {

    //GroupID tempID = new GroupID();
    IntWritable outputInt = new IntWritable(Integer.MIN_VALUE);

    // now we need to parse the variable dimensions out
    int[] variableShape = Utils.getVariableShape( context.getConfiguration());
    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                  variableShape.length);

    int neededSize = Utils.calcTotalSize( extractionShape );

    HolisticResult maxVal =  new HolisticResult();
    maxVal.setNeededValueCount( neededSize );

    for (HolisticResult value : values) {
      // sanity check
      if ( maxVal.isFull() ) {
        LOG.warn("Adding an element to an already full HR. Key: " + 
                 key.toString() +
                 " array size: " + maxVal.getNeededValueCount() + 
                 " current elems: " +
                 maxVal.getCurrentValueCount() );
      } 

      LOG.info("GID: " + key + " merging in " + value.getCurrentValueCount() + 
               " keys, already " + maxVal.getCurrentValueCount() +  " present");

      maxVal.merge(value);
    }

    // now, the remainig holistic result should be full. Check though
    // and make sure it wasn't already finalized
        
    if( maxVal.isFull() && !maxVal.isFinal() ) {
      // apply whatever function you want, 
      // in this case we sort and then pull the median out
      maxVal.sort();

      if ( !Utils.isSorted( maxVal.getValues() )) {
        LOG.error("Holistic result for GID: " + key + " has unsorted results");
      }

      maxVal.setFinal( maxVal.getValues()[(maxVal.getValues().length)/2] );
      //LOG.info("gid: " + key + " is full at " + 
      //         maxVal.getCurrentValueCount() + " elements");
      context.getCounter(MedianReducerStatus.FULL).increment(1);
    } else if (!maxVal.isFull() ) {
      LOG.info("gid: " + key + " has " + maxVal.getCurrentValueCount() + 
               " elements" +
               " but should be full");
      context.getCounter(MedianReducerStatus.NOTFULL).increment(1);
    } else if (maxVal.isFinal() ) {
      LOG.info("gid: " + key + " has already been set to final"); 
      context.getCounter(MedianReducerStatus.NOTFINAL).increment(1);
    }
        

    //tempID.setGroupID( tempID.unflatten(variableShape, key.get() ) );

    outputInt.set(maxVal.getValue(0));
    context.write(key, outputInt);
  }
}
