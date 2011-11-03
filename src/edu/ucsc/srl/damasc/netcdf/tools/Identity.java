package edu.ucsc.srl.damasc.netcdf.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucsc.srl.damasc.netcdf.combine.AverageCombiner;
import edu.ucsc.srl.damasc.netcdf.combine.MedianCombiner;
import edu.ucsc.srl.damasc.netcdf.combine.SimpleMedianCombiner;
import edu.ucsc.srl.damasc.netcdf.combine.MaxCombiner;
import edu.ucsc.srl.damasc.netcdf.combine.SimpleMaxCombiner;
import edu.ucsc.srl.damasc.netcdf.io.input.NetCDFFileInputFormat;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.HolisticResult;
import edu.ucsc.srl.damasc.netcdf.io.AverageResult;
import edu.ucsc.srl.damasc.netcdf.io.Result;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.map.AverageMapper;
import edu.ucsc.srl.damasc.netcdf.map.MaxMapper;
import edu.ucsc.srl.damasc.netcdf.map.MedianMapper;
import edu.ucsc.srl.damasc.netcdf.map.NullMapper;
import edu.ucsc.srl.damasc.netcdf.map.SimpleMaxMapper;
import edu.ucsc.srl.damasc.netcdf.map.SimpleMedianMapper;
import edu.ucsc.srl.damasc.netcdf.reduce.AverageReducer;
import edu.ucsc.srl.damasc.netcdf.reduce.MaxReducer;
import edu.ucsc.srl.damasc.netcdf.reduce.MedianReducer;
import edu.ucsc.srl.damasc.netcdf.reduce.NullReducer;
import edu.ucsc.srl.damasc.netcdf.reduce.SimpleMaxReducer;
import edu.ucsc.srl.damasc.netcdf.reduce.SimpleMedianReducer;
import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.Utils.Operator;

public class Identity extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: identity <input> <output>");
			System.exit(2);
		}

    //Configuration conf = new Configuration();
		Configuration conf = getConf();
    //JobConf jc = new JobConf(conf, Identity.class);
    //Cluster cluster = new Cluster(conf);
    //Job job = Job.getInstance(cluster);
    Job job = new Job(conf);
    String jobNameString = "";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    if( Utils.getOperator(conf) == Operator.simpleMedian) {
      jobNameString += "Simple Median";
      job.setJarByClass(Identity.class);

      job.setMapperClass(SimpleMedianMapper.class);
      if ( Utils.useCombiner(conf) ) {
        jobNameString += " with combiner ";
	      job.setCombinerClass(SimpleMedianCombiner.class);
      }
	    job.setReducerClass(SimpleMedianReducer.class);
	
	    // mapper output
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(HolisticResult.class);

	    // reducer output
	    job.setOutputKeyClass(GroupID.class);
	    job.setOutputValueClass(IntWritable.class);
	
    } else if( Utils.getOperator(conf) == Operator.median) {
      jobNameString += "Median";
      job.setJarByClass(Identity.class);

      job.setMapperClass(MedianMapper.class);
      if ( Utils.useCombiner(conf) ) {
        jobNameString += " with combiner ";
	      job.setCombinerClass(MedianCombiner.class);
      }
	    job.setReducerClass(MedianReducer.class);
	
	    // mapper output
	    job.setMapOutputKeyClass(GroupID.class);
	    job.setMapOutputValueClass(HolisticResult.class);

	    // reducer output
	    job.setOutputKeyClass(GroupID.class);
	    job.setOutputValueClass(IntWritable.class);
    } else if( Utils.getOperator(conf) == Operator.simpleMax) {
      jobNameString += "Simple Max";
      job.setJarByClass(Identity.class);
      job.setMapperClass(SimpleMaxMapper.class);
	    job.setReducerClass(SimpleMaxReducer.class);

      if ( Utils.useCombiner(conf) ) {
        jobNameString += " with combiner ";
	      job.setCombinerClass(SimpleMaxCombiner.class);
      }
	
	    // mapper output
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // reducer output
	    job.setOutputKeyClass(GroupID.class);
	    job.setOutputValueClass(IntWritable.class);
	
    } else if( Utils.getOperator(conf) == Operator.max) {
      jobNameString += "max";
      job.setJarByClass(Identity.class);
      job.setMapperClass(MaxMapper.class);

      if ( Utils.useCombiner(conf) ) {
        jobNameString += " with combiner ";
	      job.setCombinerClass(MaxCombiner.class);
      }

	    job.setReducerClass(MaxReducer.class);
	
	    // mapper output
	    job.setMapOutputKeyClass(GroupID.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // reducer output
	    job.setOutputKeyClass(GroupID.class);
	    job.setOutputValueClass(IntWritable.class);
	
    } else if( Utils.getOperator(conf) == Operator.nulltest) {
      jobNameString += "null test";
      job.setJarByClass(Identity.class);
      job.setMapperClass(NullMapper.class);
	    job.setReducerClass(NullReducer.class);
	
	    // reducer output
	    job.setOutputKeyClass(GroupID.class);
	    job.setOutputValueClass(IntWritable.class);
	
	    // mapper output
	    job.setMapOutputKeyClass(GroupID.class);
	    job.setMapOutputValueClass(IntWritable.class);
    } else if( Utils.getOperator(conf) == Operator.average) {
      jobNameString += " average ";
      job.setJarByClass(Identity.class);
      job.setMapperClass(AverageMapper.class);
	    job.setReducerClass(AverageReducer.class);
	
	    // reducer output
	    job.setOutputKeyClass(GroupID.class);
	    //job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	
	    // mapper output
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(AverageResult.class);
    } else { // TODO -jbuck error out here, do NOT assume a default functor

      System.err.println("No operator specified. Try again");
			System.exit(2);
    }

    if( Utils.noScanEnabled(conf) ) 
      jobNameString += " with noscan ";

    if( Utils.queryDependantEnabled(conf) ) 
      jobNameString += " and query dependant";

    jobNameString += Utils.getPartModeString(conf) + ", " + 
                     Utils.getPlacementModeString(conf);
    jobNameString += " with " + Utils.getNumberReducers(conf) + 
                     " reducers ";

    job.setJobName(jobNameString);

    job.setInputFormatClass(NetCDFFileInputFormat.class);
    job.setNumReduceTasks( Utils.getNumberReducers(conf) );

    NetCDFFileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Identity(), args);
		System.exit(res);
	}
}
