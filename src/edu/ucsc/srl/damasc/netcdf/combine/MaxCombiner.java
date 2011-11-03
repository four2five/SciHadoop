package edu.ucsc.srl.damasc.netcdf.combine;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.Result;

/**
 * Combiner for the max operator
 */
public class MaxCombiner extends 
       Reducer<GroupID, IntWritable, GroupID, IntWritable> {

  private static final Log LOG = LogFactory.getLog(MaxCombiner.class);

  /**
   * Reduces values for a given key
   * @param key the Key for the given values being passed in
   * @param values a List of IntWritable objects to combine
   * @param context the Context object for the currently executing job
   */
  public void reduce(GroupID key, Iterable<IntWritable> values, 
                     Context context)
                     throws IOException, InterruptedException {

    
    IntWritable maxVal = new IntWritable();
    maxVal.set(Integer.MIN_VALUE);

    for (IntWritable value : values) {
      if ( value.get() > maxVal.get() )
        maxVal.set(value.get());
    }

    context.write(key, maxVal);
  }
}
