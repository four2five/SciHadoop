package edu.ucsc.srl.damasc.netcdf.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.netcdf.io.AverageResult;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * A reducer for the Average function
 */
public class AverageReducer extends 
        Reducer<LongWritable, AverageResult, GroupID, IntWritable> {

  private static final Log LOG = LogFactory.getLog(AverageReducer.class);

  /**
   * Reduces all the values for the given key, produces the average of the
   * AverageResult objects in values
   * @param key the flattened corner for this instance of the extraction shape 
   * in the global logical space
   * @param values an Iterable list of AverageResult objects that represent all the inputs
   * for this key
   * @param context the Context object for the executing program
   */
  public void reduce(LongWritable key, Iterable<AverageResult> values, 
                     Context context)
                     throws IOException, InterruptedException {

    long timer = System.currentTimeMillis();

    int[] variableShape = Utils.getVariableShape( context.getConfiguration());
    AverageResult avgResult = new AverageResult();
    GroupID myGroupID = new GroupID();
    IntWritable myIntW = new IntWritable();

    //for (Result value : values) {
    for (AverageResult value : values) {
      //currentAverage = (int) ((((long)currentAverage * currentSamplesInAverage) + tempValue) / (currentSamplesInAverage++));
      avgResult.addAverageResult(value);
    }

    myGroupID.setGroupID( myGroupID.unflatten(variableShape, key.get()) );
    myIntW.set(avgResult.getCurrentValue());
    context.write(myGroupID, myIntW);
    
    timer = System.currentTimeMillis() - timer;
    LOG.info("total reducer took: " + timer + " ms");
  }
}
