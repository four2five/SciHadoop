package edu.ucsc.srl.damasc.netcdf.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * Reducer that simply iterates through the data it is passed
 */
public class NullReducer extends 
        Reducer<GroupID, IntWritable, GroupID, IntWritable> {

  private static final Log LOG = LogFactory.getLog(NullReducer.class);

  /**
   * Iterates through the data it is passed, doing nothing to it. Outputs a 
   * Integer.MINIMUM_VALUE as the value for its key
   * @param key the flattened corner for this instance of the extraction shape 
   * in the global logical space
   * @param values an Iterable list of IntWritable objects that represent all the inputs
   * for this key
   * @param context the Context object for the executing program
   */
  public void reduce(GroupID key, Iterable<IntWritable> values, 
                     Context context)
                     throws IOException, InterruptedException {

    long timer = System.currentTimeMillis();

    IntWritable maxVal = new IntWritable();
    maxVal.set(Integer.MIN_VALUE);

    // empty loop
    for (IntWritable value : values) {
    }

    context.write(key, maxVal);

    timer = System.currentTimeMillis() - timer;
    LOG.info("total reducer took: " + timer + " ms");
  }
}
