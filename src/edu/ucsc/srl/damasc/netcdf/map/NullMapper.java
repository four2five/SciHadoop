package edu.ucsc.srl.damasc.netcdf.map;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;

import ucar.ma2.Array;
import ucar.ma2.ArrayInt;

/**
 * Dummy mapper, just passed data through with a dummy key.
 * This is used for testing purposes
 */
public class NullMapper extends Mapper<ArraySpec, Array, GroupID, IntWritable> {

 /**
 * Reduces values for a given key
 * @param key ArraySpec representing the given Array being passed in
 * @param value an Array to process that corresponds to the given key 
 * @param context the Context object for the currently executing job
 */
  public void map(ArraySpec key, Array value, Context context)
                  throws IOException, InterruptedException {
    try {
      ArrayInt intArray = (ArrayInt)value;
      int[] dummyGID = {0};
      GroupID groupID = new GroupID(dummyGID, "nullData");
      IntWritable intW = new IntWritable(Integer.MIN_VALUE);

      context.write(groupID, intW);
    } catch ( Exception e ) {
      System.out.println("Caught an exception in NullMapper.map()" + e.toString() );
    }
  }
}
