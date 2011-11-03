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

import edu.ucsc.srl.damasc.netcdf.combine.MedianCombiner;
import edu.ucsc.srl.damasc.netcdf.io.input.NetCDFFileInputFormat;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.HolisticResult;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.map.MedianMapper;
import edu.ucsc.srl.damasc.netcdf.reduce.MedianReducer;
import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.Utils.Operator;

public class Median extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: median <input> <output>");
			System.exit(2);
		}

		Configuration conf = getConf();
    Job job = new Job(conf);
    String jobNameString = "";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    jobNameString += "Median";
    job.setJarByClass(Median.class);

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
		int res = ToolRunner.run(new Configuration(), new Median(), args);
		System.exit(res);
	}
}
