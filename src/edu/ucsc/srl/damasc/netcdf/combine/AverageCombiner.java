package edu.ucsc.srl.damasc.netcdf.combine;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.AverageResult;

/**
 * Combiner class for the Average function
 */
public class AverageCombiner extends 
       Reducer<LongWritable, AverageResult, LongWritable, AverageResult> {

  private static final Log LOG = LogFactory.getLog(AverageCombiner.class);

  /**
   * Reduces values for a given key
   * @param key the Key for the given values being passed in
   * @param values a List of AverageResult objects to combine
   * @param context the Context object for the currently executing job
   */
  public void reduce(LongWritable key, Iterable<AverageResult> values, 
                     Context context)
                     throws IOException, InterruptedException {


    AverageResult avgResult = new AverageResult();

    for (AverageResult value : values) {
      avgResult.addAverageResult(value);
    }

    context.write(key, avgResult);
  }
}
