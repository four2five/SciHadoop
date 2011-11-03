package edu.ucsc.srl.damasc.netcdf.combine;

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
 * Combiner class for the Median operator
 */
public class MedianCombiner extends 
        Reducer<GroupID, HolisticResult, GroupID, HolisticResult> {

  private static final Log LOG = LogFactory.getLog(MedianCombiner.class);
  static enum MedianCombinerStatus { FULL, NOTFULL, MERGED }

  /**
   * Reduces values for a given key
   * @param key the Key for the given values being passed in
   * @param values a List of HolisticResult objects to combine
   * @param context the Context object for the currently executing job
   */

  public void reduce(GroupID key, Iterable<HolisticResult> values, 
                     Context context)
                     throws IOException, InterruptedException {


    // now we need to parse the variable dimensions out
    int[] variableShape = Utils.getVariableShape( context.getConfiguration());
    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 variableShape.length);
    int neededValues = Utils.calcTotalSize(extractionShape);
    GroupID tempID = new GroupID();

    HolisticResult holVal = new HolisticResult();
    holVal.setNeededValueCount( neededValues );

    for (HolisticResult value : values) {
      if ( holVal.isFull() ) {
        LOG.warn("Adding an element to an already full HR. Key: " + 
                 key.toString() + 
                 " array size: " + holVal.getNeededValueCount() + 
                 " current elems: " + 
                 holVal.getCurrentValueCount() );
      }

      holVal.merge(value);
      context.getCounter(MedianCombinerStatus.MERGED).increment(value.getCurrentValueCount());
    }

    // now, the remainig holistic result should be full. Check though
    if( holVal.isFull() ) {
      // apply whatever function you want, in this case we 
      // sort and then pull the median out
      holVal.sort();
      holVal.setFinal( holVal.getValues()[(holVal.getValues().length)/2] );
      context.getCounter(MedianCombinerStatus.FULL).increment(1);
    } else {                                                                
      context.getCounter(MedianCombinerStatus.NOTFULL).increment(1);
    }

    context.write(key, holVal);
  }
}
