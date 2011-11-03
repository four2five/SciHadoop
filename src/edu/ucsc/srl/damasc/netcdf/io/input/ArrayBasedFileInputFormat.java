package edu.ucsc.srl.damasc.netcdf.io.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Exception;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.lang.Math;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.NcHdfsRaf;
import edu.ucsc.srl.damasc.netcdf.io.SHFileStatus;
import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.Utils.MultiFileMode;
import edu.ucsc.srl.damasc.netcdf.Utils.PartMode;
import edu.ucsc.srl.damasc.netcdf.Utils.PlacementMode;

/**
 * an abstract class that servers as a super class for 
 * File Input Formats that store array-based data (or data that can be addressed 
 * as if it were array-based). 
 */
public abstract class ArrayBasedFileInputFormat
    extends FileInputFormat {

  // Used for logging in this class
  private static final Log LOG = LogFactory.getLog(ArrayBasedFileInputFormat.class);

  /**
   * Helper function to add an ArraySpec to a HashMap that stores 
   * ArraySpec -> BlockLocation mappings
   * @param blockToAS HashMap that stores the mappings being added to
   * @param offset The offset, in bytes, in the file that this ArraySpec starts
   * at
   * @param as The ArraySpec to add to the Map
   */
  public static void insertNewAs( HashMap<BlockLocation, 
                                  ArrayList<ArraySpec>> blockToAS, 
                                  long offset, ArraySpec as) {

    // search for the correct BlockLocation 
    // (TODO this is inefficient, fix it)
    Iterator iter = blockToAS.keySet().iterator();

    while( iter.hasNext() ) {
      BlockLocation tempKey = (BlockLocation)(iter.next());
      if( tempKey.getOffset() == offset ) {
        (blockToAS.get(tempKey)).add(as);
      }
    }
  }

  /**
   * Partitions the data represented by dims into groups of records where records 
   * are whole subarrays with size 1 on the zero-th dimension.
   * This may not work for all formats, revisit this later TODO
   * @param dims List of Dimension objects representing the dimensions of the input
   * data that we are generating partitions for
   * @param varName Name of the variable we are generating partitions for
   * @param fileName name of the file that contains the variable we are generating
   * partitions for
   * @param partMode the partitioning mode being used to generate the partitions
   * @param startOffset the logical offset in the input data to start creating
   * partitions at
   * @param conf Configuration object for this execution the given MR program
   * @return an array of ArraySpec objects that represent the partitions this
   * function generated
   */
  protected ArraySpec[] recordBasedPartition( 
                          int[] dims, 
                          String varName, String fileName,
                          PartMode partMode,
                          int[] startOffset,
                          Configuration conf) throws IOException {

    int ndims = dims.length;
    int recDimLen = dims[0];

    int[] recordShape = new int[ndims];
    int[] recordCorner = new int[ndims];

    ArrayList<ArraySpec> records = new ArrayList<ArraySpec>(recDimLen);

    for (int i = 0; i < ndims; i++) {
      recordShape[i] = dims[i];
      recordCorner[i] = 0;
    }

    recordShape[0] = 1;

    if ( Utils.queryDependantEnabled(conf) ) {
      LOG.info("Query Dependant enabled");
      recordShape[0] = Utils.getExtractionShape(conf, recordShape.length)[0];
    } else {
      LOG.info("Query Dependant NOT enabled");
    }


    ArraySpec tempSpec = null;
    for (int i = 0; i < recDimLen; i+=recordShape[0]) {
      recordCorner[0] = i;
      // FIXME: this is clunky 
      try {
        // if this is optC and the record is not valid, do not add it, 
        if ( Utils.noScanEnabled(conf)) { 
          if ( Utils.isValid(recordCorner, conf) ) {
            tempSpec = new ArraySpec(recordCorner, recordShape, varName, fileName);
            tempSpec.setLogicalStartOffset(startOffset);
            records.add(tempSpec);
            //records.add(new ArraySpec(recordCorner, recordShape, varName, fileName));
          }
        } else { // else wise do add it
          tempSpec = new ArraySpec(recordCorner, recordShape, varName, fileName);
          tempSpec.setLogicalStartOffset(startOffset);
          records.add(tempSpec);
          //records.add(new ArraySpec(recordCorner, recordShape, varName, fileName));
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    
    ArraySpec[] returnArray = new ArraySpec[records.size()];
    returnArray = records.toArray(returnArray);

    return returnArray;
  }

  /**
   * Calculates the size of each parition when the proportional 
   * partitioning scheme is used.
   * @param dims represent the logical input space
   * @param blockSize the size, in bytes, of the blocks used to
   * store the file that contains the data being partitioned
   * @param numBlocks the number of blocks to use for generating
   * the per-partition size
   * @param fileLen the length of the file, in bytes
   * @param dataTypeSize the size, in bytes, of a single cell for
   * the given data type stored in the file for which partitions 
   * are being generated
   * @param conf Configuration object for this current MR program
   * @return an int array that is the same length as dims, where each
   * element is the length, in cells, that the step shape is in the
   * given dimension
   */

  private int[] calcStepShape( int[] dims, long blockSize, 
                                long numBlocks, long fileLen, 
                                int dataTypeSize, Configuration conf ) {

    int[] stepShape = new int[dims.length]; // sort out the max space

    for ( int i=0; i<dims.length; i++) {
      stepShape[i] = dims[i];
    }

    stepShape[0] = Math.round( Math.max(1, stepShape[0] / numBlocks) );

    // if holistic functions are turned on, we need to make 
    // sure this encompasses enough records
    // Also need to ensure that it ends up a 
    // being a multiple of the zero-dimension of extraction shape
    if ( Utils.queryDependantEnabled(conf) ) {
      int numExShapesInStep  = 
        Math.max( (stepShape[0] / 
        Utils.getExtractionShape(conf, stepShape.length)[0]), 1);
      stepShape[0] = 
        numExShapesInStep *  
        Utils.getExtractionShape(conf, stepShape.length)[0];
    }

    return stepShape;
  }

  /**
   * The partitioning scheme creates partitions distributes the data
   * to be read (approximately) evenly over all the blocks in the file.
   * This is a very naive approach and should not be used other than as a 
   * point of refernece.
   * @param dims represent the logical input space
   * @param varName name of the variable that we're creating 
   * partitions for
   * @param blockSize the size, in bytes, of the blocks used to
   * store the file that contains the data being partitioned
   * @param numBlocks the number of blocks to use for generating
   * the per-partition size
   * @param fileLen the length of the file, in bytes
   * @param dataTypeSize the size, in bytes, of a single cell for
   * the given data type stored in the file for which partitions 
   * are being generated
   * @param fileName name of the file that partitions are being generated for
   * @param startOffset the logical position in the file to begin generating
   * partitions from
   * @param conf Configuration object for this current MR program
   * @return an array of ArraySpecs that is the same length as dims, where each
   * ArraySpec corresponds to a partition
   */
  protected ArraySpec[] proportionalPartition( int[] dims, 
                                             String varName, long blockSize, 
                                             long numBlocks, 
                                             long fileLen, int dataTypeSize,
                                             String fileName,
                                             int[] startOffset,
                                             Configuration conf ) 
                                            throws IOException {
    int ndims = dims.length;
    ArrayList<ArraySpec> records = new ArrayList<ArraySpec>();

    // this next bit is fairly hard-coded and specific to our tests. 
    // it represents a naive split that a human might come up with

    // sort out the step size 
    int[] stepShape = calcStepShape(dims, blockSize, numBlocks, 
                                     fileLen, dataTypeSize, conf);

    int[] tempCorner = new int[ndims];
    int[] tempStep = new int[ndims];

    // initialize the temporary step shape to be the first step
    for( int i=0; i<ndims; i++ ) {
      tempStep[i] = stepShape[i];
      tempCorner[i] = 0;
    }

    LOG.info("Calculated stepshape: " + Utils.arrayToString(stepShape) );

    ArraySpec tempSpec = new ArraySpec();
    int stepSize = tempStep[0];

    // create the actual splits
    while ( tempCorner[0] < dims[0] ) {
      try { 
        if( Utils.noScanEnabled(conf) ) {
          if( Utils.isValid(tempCorner, conf)) {  
            tempSpec = new ArraySpec( tempCorner, tempStep, varName, fileName);
            tempSpec.setLogicalStartOffset(startOffset);
            records.add(tempSpec);
          }
        } else { 
          tempSpec = new ArraySpec( tempCorner, tempStep, varName, fileName);
          tempSpec.setLogicalStartOffset(startOffset);
          records.add(tempSpec);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }

      // update the corner
      tempCorner[0] += stepSize;

      // use MIN here to make sure we don't over run the constraining space
      stepSize = Math.min(tempStep[0], 
                          dims[0] - tempCorner[0] );

      // update the shape of the next write appropriately
      tempStep[0] = stepSize;
    }   

    ArraySpec[] returnArray = new ArraySpec[records.size()];
    returnArray = records.toArray(returnArray);

    return returnArray;
  }

  // FF specific, used by sampling method
  /*
  private BlockLocation offsetToBlock(BlockLocation[] blocks, long offset) {
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      if (start <= offset && offset < end)
        return block;
    }
    return null;
  }
  */

  /**
   * This places partitions across all blocks, in a round robin fasion,
   * starting with the first block in the file.
   * @param records an array of partitions, represented as ArraySpecs, to 
   * map to HDFS blocks
   * @param blocks list of BlockLocations that represent the blocks storing
   * the data to be processed
   * @param blockToArrays the map that stores the mappings of ArraySpec(s)
   * to BlockLocations. This is effectively what is produced by this method
   * @param totalArraySpecCount a count of how many ArraySpecs need to be placed.
   * @param fileName name of the file that stores that data to be processed
   * @param conf Configuration object representing the current execution of 
   * this MR program
   * @return the totalArraySpecCount, which is the number of ArraySpec's seen
   * so far 
   */
  protected long roundRobinPlacement( 
                  ArraySpec[] records, BlockLocation[] blocks,
                  HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays, 
                  long totalArraySpecCount, String fileName,
                  Configuration conf ) 
                  throws IOException {

    // place each ArraySpec in the correct block
    for (ArraySpec record : records) {
      int blockToInsertTo = 
          (int) (totalArraySpecCount % blockToArrays.size());
          /*
      System.out.println("record corner: " + 
                         Utils.arrayToString(record.getCorner() ) + 
                         " going into block " +
                         blockToInsertTo + " which starts at offset " + 
                          blocks[blockToInsertTo] );
      */

      blockToArrays.get( blocks[blockToInsertTo] ).add(record);
      totalArraySpecCount++;
    }

    return totalArraySpecCount;
  }

 /* 
  public List<SHFileStatus> orderMultiFileInput( List<FileStatus> files/
                                                 Configuration conf ) { 
    List<SHFileStatus> retList = new ArrayList<SHFileStatus>();

    // first, sort the files in alphanumeric order
    Collections.sort(files);

    int[] startOffset = null;
    // now go through them, in order
    for (FileStatus file: files) {

      Variable var = getVariable(file, conf); 

      if ( startOffset == null ){
        startOffset = new int[var.getDimensions().size()];
        for( int i=0; i<startOffset.length; i++) {
          startOffset[i] = 0;
        }
      }

      if ( Utils.getMultiFileMode(conf) == MultiFileMode.combine ) {
        retList.add(new SHFileStatus(file, startOffset) );
      }  else { // default to concat mode
        retList.add(new SHFileStatus(file, startOffset) );
        // add the length of *the* variable in this file to the 
        // start of the next file
        startOffset[0] += var.getDimensions().get(0).getLength();
      }
    }

    return retList;
  }
*/
  @Override
  /**
   * This method is called by the Hadoop framework to generate the 
   * splits for the currently execute MR program.
   * @param job JobContext object representing the currently 
   * executing job. Pertinent data can be extracted from it to 
   * determine how to proceed. 
   * @return a List of InputSplits which will be passed out to 
   * Data Nodes to execute
  */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    //List<FileStatus> files = listStatus(job);

   // HashMap<Long, ArrayList<ArraySpec>> blockToSlab =
   //   new HashMap<Long, ArrayList<ArraySpec>>();

/*
    FileStatus ncfileStatus = null;


    for (FileStatus file: files) {
      if (ncfileStatus == null) {
        ncfileStatus = file;
        LOG.info("Using input: " + file.getPath().toString());
      } else {
        LOG.warn("Skipping input: " + file.getPath().toString());
      }
    }

    if (ncfileStatus == null)
      return splits;
*/
/*
    PartMode partMode = Utils.getPartMode(job.getConfiguration());
    PlacementMode placementMode = 
        Utils.getPlacementMode(job.getConfiguration());
*/

    /*
    if (Utils.getMultiFileMode(job.getConfiguration()) == MultiFileMode.concat) {
      orderMultiFileInput( files, shFiles);
    }
    */

    // set the starting offset for each file (depends on damasc.multi_file_mode 
    /*
    shFiles = orderMultiFileInput( files, job.getConfiguration() );

    for (SHFileStatus shFile: shFiles) {
      LOG.info("Parsing file: " + shFile.getFileStatus().getPath().toString());
      Utils.addFileName(shFile.getFileStatus().getPath().toString(), job.getConfiguration());
      genFileSplits(job, shFile, splits, partMode, placementMode);
    }

    
    // debug: log splits to a file if the debug log files is set
    String debugFileName = Utils.getDebugLogFileName(job.getConfiguration());
    if ( "" != debugFileName ) {  
      LOG.info("Trying to log to " + debugFileName);
      File outputFile = new File( debugFileName );
      BufferedWriter writer = new BufferedWriter( new FileWriter(outputFile));

      int i = 0;
      for (InputSplit split : splits) {
        ArrayBasedFileSplit tempSplit = (ArrayBasedFileSplit)split;
        //LOG.info("Split " + i);
        writer.write("Splits " + i);
        writer.newLine();
        for ( ArraySpec spec : tempSplit.getArraySpecList() ) {
          writer.write("File: " + spec.getFileName() + 
                       "\tvar: " + spec.getVarName() + 
                       "\tcorner: " + Utils.arrayToString( spec.getCorner()) + 
                       "\t shape: " + Utils.arrayToString( spec.getShape() ) + 
                       "\t startOffset: " + Utils.arrayToString( spec.getLogicalStartOffset()) );
          writer.newLine();
        }
        i++;
      }
      writer.close();
    } else {
      LOG.info("No debugFileName set");
    }
    */

    return splits;
  }
}
