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
 * Reducer for the Max operator
 */
public class MaxReducer extends 
        Reducer<GroupID, IntWritable, GroupID, IntWritable> {

  private static final Log LOG = LogFactory.getLog(IdentityReducer.class);

  /**
   * Reduces all the values for the given key, produces the maximum of the
   * IntWritable objects in values
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

    // now we need to parse the variable dimensions out
    //int[] variableShape = Utils.getVariableShape( context.getConfiguration());

    IntWritable maxVal = new IntWritable();
    maxVal.set(Integer.MIN_VALUE);

    for (IntWritable value : values) {
      if ( value.get() > maxVal.get() )
        maxVal.set(value.get());
    }

    //tempID.setGroupID( tempID.unflatten(variableShape, key.get() ) );
    context.write(key, maxVal);

    timer = System.currentTimeMillis() - timer;
    LOG.info("total reducer took: " + timer + " ms");
  }
}
