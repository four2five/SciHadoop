package edu.ucsc.srl.damasc.netcdf.io.input;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.Dimension;
import ucar.ma2.Array;
import ucar.ma2.ArrayLong;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.NcHdfsRaf;
import edu.ucsc.srl.damasc.netcdf.io.SHFileStatus;
import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.Utils.PartMode;
import edu.ucsc.srl.damasc.netcdf.Utils.PlacementMode;
import edu.ucsc.srl.damasc.netcdf.Utils.MultiFileMode;


/**
 * FileInputFormat class that represents NetCDF files (specifically NetCDF v3)
 * Format specific code goes here
 */
public class NetCDFFileInputFormat 
    extends ArrayBasedFileInputFormat {

  private static final Log LOG = 
      LogFactory.getLog(NetCDFFileInputFormat.class);

  /**
   * Use FileInputFormat for file globbing capabilities, and filter out any
   * files that cannot be opened by NetcdfFile.open().
   * @param jobC Context for the job being executed
   * @return a List<FileStatus> where each entry represents a valid file
   */
  @Override
  protected List<FileStatus> listStatus(JobContext jobC) throws IOException {

    //List<FileStatus> files = new ArrayList<FileStatus>(super.listStatus(job));
    List<FileStatus> files = 
        super.listStatus(jobC);
    ArrayList<FileStatus> rfiles = new ArrayList<FileStatus>();

    for (FileStatus file: files) {
      NcHdfsRaf raf = new NcHdfsRaf(file, jobC.getConfiguration());

      try {
        NetcdfFile ncfile = NetcdfFile.open(raf, file.getPath().toString());
      } catch (IOException e) {
        LOG.warn("Skipping input: " + e.getMessage());
        rfiles.add(file);
      }
    }

    files.removeAll(rfiles);
    return files;
  }

  /** 
   * This is somewhat of a utility function in that it 
   * figures out what is the current, highest numbered dimension
   * that is not "full". This function is exclusively used by 
   * the method nextCalculatedPartition().
   * @param cellsLeft how many cells that still need to be covered by the
   * shape being generated
   * @param strides How many cells it takes to iterate a given dimension,
   * The lengths correspond to the given dimension (strides[0] = number
   * of cells needed to iterate on dimension zero, etc.)
   * @param current the current position, in the logical space
   * @param varShape the shape of the variable being processed
   * @return the dimension that should next be iterated on when creating
   * partition shapes
   */
  public static int calcCurrentDim( long cellsLeft, long[] strides, 
                                    int[] current, int[] varShape ) {
    int retDim = -1;

    // first, see if we need to fill out any dimensions above zero
    for ( int i = current.length - 1; i > 0; i-- ) { 
      if (current[i] == 0)   
      {
        continue;   // no need to increment this level, it's full
      } else if ( cellsLeft > strides[i] ) { 
        // if this dim is non-zero, non-full and there are 
        // sufficient cells left, it's out winner
        retDim = i;
        return retDim;
      }
    }

    // if we're still in this fucntion, 
    //it's time to start filling in, from dim-0 down,
    // as free cells permit
    for( int i=0; i < current.length; i++ ) {
      if ( strides[i] <= cellsLeft ) { 
        retDim = i;
        return retDim;
      }
    }

    // if we get here, something went super wrong. Return -1 to indicate such
      retDim = -1;
      return retDim;
  }

  /**
   * This method iteratively creates partition shapes. It relies heavily on the
   * function calcCurrentDim().
   * @param var Variable object for the current NetCDF variable that we're 
   * generating partitions for
   * @param varShape the shape of the variable, by dimension, that we're 
   * creating partitions for
   * @param currentStart where to start creating a partition from
   * @param blockSize the HDFS block size (in bytes)
   * @param dataTypeSize the size, in bytes, of the data type stored in
   * the variable that partitions are being generated for
   * @param allOnes a helper structure. It's simply an array that is the same 
   * length as varshape that contains all "1"s 
   * @param strides The number of cells require to iterate a given dimension
   * @param fileName The name of the file that contains the variable that 
   * partitions are being generated for
   * @param blockToAS a HashMap that stores the mapping of ArraySpecs to
   * file system blocks
   * @param startOffset deprecated
   * @param conf Configuration object for the currently executing MR job
   *
   */
  public static void nextCalculatedPartition( 
                      Variable var, int[] varShape, 
                      int[] currentStart, 
                      long blockSize, 
                      int dataTypeSize, 
                      int[] allOnes, long[] strides, 
                      String fileName,
                      HashMap<BlockLocation, ArrayList<ArraySpec>> blockToAS,
                      int[] startOffset,
                      Configuration conf ) 
                      throws IOException, InvalidRangeException, Exception {

    if ( Utils.endOfVariable(varShape, currentStart) ) { 
      return;
    }

    // get the byte-offset of the current starting point
    ArrayLong offsets = var.getLocalityInformation(
                        currentStart, 
                        allOnes);

    long curLocation = offsets.getLong(0);
    long bytesLeft = blockSize - (curLocation % blockSize);
    long cellsLeft = bytesLeft / dataTypeSize;
    long currentBlock = curLocation / blockSize;


    // determine the highest, non-full block
    int curDim = calcCurrentDim( cellsLeft, strides, currentStart, varShape );

    if ( curDim < 0) {
      System.out.println("DANGER:\n" + 
      "\t curDim: " + curDim + 
      "\tcells left: " + cellsLeft + "\n" + 
      "\tstrides: " + Utils.arrayToString(strides) );
    }

    int steps = (int)Math.min( varShape[curDim] - currentStart[curDim], 
                               cellsLeft /strides[curDim] );

    int[] readShape = new int[currentStart.length];

    // readShape[curDim] will be updated but I wanted to init it properly
    for ( int i = 0; i < currentStart.length; i++) { 
      readShape[i] = 1;
    }

    // if we're stepping the record dim, we need to be 
    // careful due to interleaved variables
    if ( curDim == 0 ) {     
      int[] tempShape = new int[currentStart.length];
      for ( int j = 0; j < tempShape.length; j++) {
        if ( j > 0 ) {
          tempShape[j] = 1;
        } else {
          tempShape[j] = currentStart[j];
        }
      }
             
      // get the current block so we know if the next 
      // record is on a differnet block
      offsets = var.getLocalityInformation(
                          currentStart, 
                          allOnes);
      curLocation = offsets.getLong(0);
      currentBlock = curLocation / blockSize;


      // j starts at 1, since we know a read at 
      // corner: currentStart[0], 1 ... will be on the current block
      for ( int j = 1; j < steps; j++) {
        tempShape[curDim]++; //  look ahead a record
        offsets = var.getLocalityInformation(
                          tempShape, 
                          allOnes);
        curLocation = offsets.getLong(0);

        bytesLeft = blockSize - (curLocation % blockSize);
        cellsLeft = bytesLeft / dataTypeSize;

        long tempBlock = curLocation / blockSize;
        // make sure this record isn't on a different block
        // and that there are enough free cells to fill it
        if ( (tempBlock != currentBlock) || 
             (cellsLeft < strides[curDim]) ){
          break;
        }
        readShape[curDim]++;
      }
    } else { // if it's not dimension 0, life is a lot easier
      readShape[curDim] = steps;
    }

    // fill in the remaining dimensions of readShape, if needed
    for ( int i = curDim + 1; i < currentStart.length; i++) {
      readShape[i] = varShape[i];
    } 


    // if noscan is on, check if this ArraySpec belongs in the MapReduce
    if ( Utils.noScanEnabled(conf)) {
      if( Utils.isValid(currentStart, conf)) {  
        insertNewAs( blockToAS, currentBlock * blockSize, 
                     new ArraySpec(currentStart, readShape, 
                     var.getName(), fileName, varShape) ); // here
      }
    } else {
      insertNewAs( blockToAS, currentBlock * blockSize, 
                   new ArraySpec(currentStart, readShape, 
                                 var.getName(), fileName, varShape) );
    }

    // increment currentDim
    currentStart[curDim] += readShape[curDim];
    
    // zero out the higher dimensions
    for ( int i = curDim+ 1; i < currentStart.length; i++ ) { 
      currentStart[i] = 0;
    }

    for ( int i = currentStart.length - 1; i > 0; i-- ) {
      if( currentStart[i] == varShape[i] ) {
        currentStart[i - 1]++;
        currentStart[i] = 0;
      }
    }

    return;
  }

  /**
   * Map an ArraySpec to a file system block. This is file format 
   * specific because it uses the sampling facility exposed by the
   * file format library. Once the correct block is determined,
   * that BlockLocation is returned.
   * @param variable The variable that partitions are being generated for
   * @param array The ArraySpec that needs to be matched to a BlockLocation
   * @param blocks A list of BlockLocations that represent the current file
   * @param conf Configuration object for the currently running MapReduce
   * program
   * @return a BlockLocation object representing the block that this 
   * ArraySpec should be assigned to
   */
  private BlockLocation arrayToBlock(Variable variable, ArraySpec array,
                                     BlockLocation[] blocks, 
                                     Configuration conf)
                                     throws IOException {

    /*
     * For each block an integer is used to accumulate the number of samples in
     * the logical space that mapped to offsets in the block.
     */
    HashMap<BlockLocation, Integer> blockHits = 
      new HashMap<BlockLocation, Integer>();

    long arraySize = array.getSize();

    int sampleSize = (int) (arraySize * Utils.getSampleRatio(conf));

    int shape[] = array.getShape();
    int rank = array.getRank();
    long[] stride = Utils.computeStrides(shape);

    /*
     * Holds the position we will query for byte offset
     */
    int[] sampleIndex = new int[rank];
    sampleIndex[0] = array.getCorner()[0];

    /*
     * Helper array (sampleIndex = corner, ones = shape)
     */
    int ones[] = new int[rank];
    for (int i = 0; i < ones.length; i++)
        ones[i] = 1;

    /*
     * Generate sampleSize positions
     *
     * FIXME:
     *   - This should be uniform without replacement
     *   - This linearization should be moved into ArraySpec
     */
    Random rand = new Random();

    for (int i = 0; i < sampleSize; i++) {

      int samplePos = rand.nextInt((int)arraySize);
      int holdSamplePos = samplePos;
      /* Convert from linearized space to coordinates */
      for (int j = 0; j < rank; j++) {
        if ( j == 0 ) {  
          sampleIndex[j] = array.getCorner()[j];
          sampleIndex[j] += samplePos / stride[j];
          samplePos -= ( sampleIndex[j] - array.getCorner()[j]) * stride[j];
        } else  { 
          sampleIndex[j] = (int)(samplePos / stride[j]);
          samplePos -= sampleIndex[j] * stride[j];
        }
      }
      
      /* 
       * Use Joe's netCDF extension to lookup the offset for this sample
       * coordinate.
       */
      Array offsetArray;

      try {

        offsetArray = variable.getLocalityInformation(sampleIndex, ones);
      } catch (Exception e) {
        throw new IOException(e.toString() + "\n corner: " + 
                              Utils.arrayToString(array.getCorner()) + 
                              " shape: " + 
                              Utils.arrayToString(array.getShape()) + 
                              " sample coord: " + 
                              Utils.arrayToString(sampleIndex));
      }

      long offset = offsetArray.getLong(0);

      /* Find block that contains this offset */
      BlockLocation block = Utils.offsetToBlock(blocks, offset);

      /* Update block-to-hits mapping */
      if (!blockHits.containsKey(block))
        blockHits.put(block, new Integer(0));

      Integer curHits = blockHits.get(block);
      blockHits.put(block, new Integer(curHits.intValue() + 1));
    }

    /* Iterator over each block-to-hits mapping entry */
    Iterator<Map.Entry<BlockLocation, Integer>> blockHitsIter;
    blockHitsIter = blockHits.entrySet().iterator();

    /* Holders for final results */
    Integer maxHits = null;
    BlockLocation maxBlock = null;

    /* Find the block with the most hits */
    while (blockHitsIter.hasNext()) {
      Map.Entry<BlockLocation, Integer> entry = blockHitsIter.next();
      BlockLocation block = entry.getKey();
      Integer count = entry.getValue();

      if (maxHits == null || count > maxHits) {
        maxHits = count;
        maxBlock = block;
      }
    }

    /* The winner */
    return maxBlock;
  }

  /**
   * Map partitions to inputSplits via sampling. This is file format specifc
   * as it uses the specific library to map a logical coordinate to an offset
   * in the byte-stream as part of the sampling function.
   * @param var Variable object that partitions are being created for
   * @param records Array of records to be mapped to blocks
   * @param blocks A list of BlockLocations that represents that file
   * that contains the variable that partitions are being generated for
   * @param blockToArrays A hash map that maps ArraySpecs to Blocks
   * @param totalArraySpecCount A count of ArraySpec objects placed
   * so far
   * @param fileName The path of the file that contains the variable 
   * that partitions are being generated for
   * @param conf Configuration object for the program currently running
   * @return  the running total of ArraySpecs seen so far
   */
  private long samplingPlacement(
                  Variable var, ArraySpec[] records, BlockLocation[] blocks,
                  HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays, 
                  long totalArraySpecCount, String fileName,
                  Configuration conf ) 
                  throws IOException {

    for (ArraySpec record : records) {
      BlockLocation block = arrayToBlock(var, record, blocks, conf);
      blockToArrays.get(block).add(record);
    }

    totalArraySpecCount++;

    return totalArraySpecCount;
  }

  /**
   * Create paritions via calculating the shape that the given scientific 
   * file format (NetCDF v3 in this case) would likely have generated, given
   * the shape of the data.
   * @param var Variable object representing the variable that partitions are
   * being generated for
   * @param blockToArrays a HashMap storing maps between BlockLocation and 
   * ArraySpecs
   * @param blockSize size of HDFS blocks, in bytes
   * @param dataTypeSize size of a single cell, in bytes
   * @param fileName name of the file containing the variable for which
   * partitions are being generated
   * @param startOffset the location in the logical space to begin generating
   * a partition for
   * @param conf Configuration object for the currently executing program
   */
  private void genCalculatedPartitions( 
                  Variable var, 
                  HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays,
                  long blockSize, int dataTypeSize, 
                  String fileName,
                  int[] startOffset,
                  Configuration conf ) 
                  throws IOException, InvalidRangeException, Exception {

    LOG.info("In genCalculatedPartitions");
    // geta list of all the dimensions
    List<Dimension> dims = var.getDimensions();

    int shape[] = new int[dims.size()];
    int allOnes[] = new int[dims.size()];
    int lastCorner[] = new int[dims.size()];

    for( int i=0; i<dims.size(); i++) {
      shape[i] = dims.get(i).getLength();
      allOnes[i] = 1;
      lastCorner[i] = 0;
    }

    long[] strides = Utils.computeStrides(shape);

    while ( !Utils.endOfVariable( shape, lastCorner)) {
      nextCalculatedPartition( var, shape, lastCorner, blockSize,
      dataTypeSize, allOnes, strides, fileName, blockToArrays, startOffset, conf);
    }
  }

  /**
   * Extract a string, containing a comma-delimited list of 
   * dimension lengths, and set it in the Configuration object
   * @param variable The current variable that partitions are 
   * being generated for
   * @param conf Configuration object for the currently executing job
   */
  private void setVarDimString( Variable variable, Configuration conf ) {
    if (!Utils.variableShapeSet() ) {
      // -jbuck buck TODO: stash variable shape into conf here
      ArrayList<Dimension> dims = 
        new ArrayList<Dimension>(variable.getDimensions());
      String varDimsString = "";

      for ( int i=0; i<dims.size(); i++) {
        if ( i > 0 )
          varDimsString += ",";

        varDimsString += dims.get(i).getLength();
      }

      Utils.setVariableShape(conf, varDimsString);
    }
  }

  /**
   * This method is called by the super class to generate
   * splits via the proscribed method. File format specific
   * calls are made from here
   * @param job Context object for the currently executing job
   * @param shFileStatus A SHFileStatus object that represents 
   * the file that splits are currently being generated for
   * @param splits A list of InputSplits that will have splits 
   * added to it as the new splits are generated.
   * @param partMode The partitioning mode to use when generating splits
   * @param placementMode the placement mode that will map splits to 
   * BlockLocations
   */
  private void genFileSplits(JobContext job, SHFileStatus shFileStatus,
                             List<InputSplit> splits, PartMode partMode, 
                             PlacementMode placementMode ) throws IOException {

    FileStatus fileStatus = shFileStatus.getFileStatus();
    Path path  = fileStatus.getPath();
    long fileLen = fileStatus.getLen();
    FileSystem fs = path.getFileSystem(job.getConfiguration());

    // Open the netCDF File.
    // FIXME: this glue should be FileSystem agnostic
    NcHdfsRaf raf = new NcHdfsRaf(fileStatus, job.getConfiguration());
    NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());

    long blockSize = fileStatus.getBlockSize();

    LOG.debug("File " + fileStatus.getPath() + " has blocksize: " + 
              blockSize + " and file size: " + fileStatus.getLen() );

    long totalArraySpecCount = 0;

    // Create and initialize the block-to-array mapping
    BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, fileLen);
    int numBlocks = blocks.length;
  
    // Create the HashMap for the result and then initialize the elements
    HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays =
      new HashMap<BlockLocation, ArrayList<ArraySpec>>();

    for (BlockLocation block : blocks) {
      blockToArrays.put(block, new ArrayList<ArraySpec>());
    }

    // Pull the dimensions out of the variable being processed
    Variable variable = getVariable(fileStatus, job.getConfiguration()); 
    setVarDimString( variable, job.getConfiguration() );
    ArrayList<Dimension> dimList =
        new ArrayList<Dimension>(variable.getDimensions());

    int[] dims = new int[dimList.size()];
    for( Dimension dim : dimList ) {
    }

    for ( int i=0; i<dimList.size(); i++) { 
      dims[i] = dimList.get(i).getLength();
    }


    int dataTypeSize = variable.getDataType().getSize();

    LOG.info("Partition mode: " + 
             Utils.getPartModeString(job.getConfiguration() ) + 
             " placement mode: " + 
             Utils.getPlacementModeString(job.getConfiguration() )
            );
    // first do the partitioning
    LOG.info("\t!!!! starting to partition for variable " + 
             variable.getName() + " !!!!!");
    ArraySpec[] records = new ArraySpec[0]; // this will hold the results

    // generate splits for the data
    switch ( partMode) { 
      case proportional: 
        records = proportionalPartition( dims, variable.getName(), 
                                         blockSize, numBlocks, fileLen, 
                                         dataTypeSize, fileStatus.getPath().toString(),
                                         shFileStatus.getStartOffset(),
                                         job.getConfiguration());
        break;

        case record:
          records = recordBasedPartition( dims, variable.getName(),
                                          fileStatus.getPath().toString(),
                                          partMode,
                                          shFileStatus.getStartOffset(),
                                          job.getConfiguration());
          break;

        case calculated:
          try {
            genCalculatedPartitions( variable, blockToArrays, blockSize, 
                                     dataTypeSize, fileStatus.getPath().toString(),
                                     shFileStatus.getStartOffset(),
                                     job.getConfiguration());
          } catch (InvalidRangeException ire ) {
            System.out.println("Caught an ire in genCalculatedPartitions\n" + 
                               ire.toString() );
          } catch (IOException ioe ) {
            System.out.println("Caught an ioe in genCalculatedPartitions\n" + 
                               ioe.toString() );
          } catch (Exception e ) {
            System.out.println("Caught an e in genCalculatedPartitions\n" + 
                               e.toString() );
          }
          break;
     }

     LOG.info("\t!!!! starting placement for variable " + 
              variable.getName() + " !!!!!");

      // now place the splits you just generated
      switch( placementMode ) {
        case roundrobin:
          if ( (records != null) && (records.length > 0) )
            totalArraySpecCount =  roundRobinPlacement(records, blocks, 
                                                       blockToArrays, 
                                                       totalArraySpecCount, 
                                                       fileStatus.getPath().toString(),
                                                       job.getConfiguration());
          break;
  
        case sampling:
          if ( (records != null) && (records.length > 0) )
            totalArraySpecCount =  samplingPlacement(variable, records, 
                                                     blocks, 
                                                     blockToArrays, 
                                                     totalArraySpecCount,
                                                     fileStatus.getPath().toString(),
                                                     job.getConfiguration());
          break;
  
        case implicit: 
        // this is a no-op, just putting this here for symetry sake
          break;
    }

    LOG.info("\t!!!! done with placement for variable " + 
             variable.getName() + " !!!!!");

     // Remove any blocks from the list with empty logical spaces
    Iterator<Map.Entry<BlockLocation, ArrayList<ArraySpec>>> blockMapIter;
    blockMapIter = blockToArrays.entrySet().iterator();

    while (blockMapIter.hasNext()) {
      Map.Entry<BlockLocation, ArrayList<ArraySpec>> entry = 
          blockMapIter.next();
      ArrayList<ArraySpec> arrays = entry.getValue();
      if (arrays.isEmpty()) {
        blockMapIter.remove();
      }
    }

    
     // Create a split for each block, and assign to this split the ArraySpec
     // instances that are associated with the block.
     // 
     // This is fundamentally our logical space decomposition that is aware of
     // the physical layout.
    blockMapIter = blockToArrays.entrySet().iterator();

    while (blockMapIter.hasNext()) {
      Map.Entry<BlockLocation, ArrayList<ArraySpec>> entry = 
          blockMapIter.next();
      ArrayList<ArraySpec> arrays = entry.getValue();
      ArrayBasedFileSplit split = 
          new ArrayBasedFileSplit(path, arrays, entry.getKey().getHosts());
      splits.add(split);
    }
  }

    /** 
     * Open the file represented by FileStatus and return a Variable
     * object for the variable specified in the job arguements
     * @param file FileStatus object representing the file from which the 
     * variable should be extracted
     * @param conf Configuration object for the currently running job
     * @return a Variable obejct representing the variable, for this job,
     * from the file specified by the file arguement
    */
    public Variable getVariable( FileStatus file, Configuration conf) { 
	    Variable retVar = null;
      try { 
	      Path path = file.getPath();
	      NcHdfsRaf raf = new NcHdfsRaf( file, conf );
	      NetcdfFile ncfile = NetcdfFile.open(raf, path.toString());
	      List<Variable> variables = ncfile.getVariables();
	
	      for (Variable variable : variables) {
	
	        // skip all but the variable we want to measure here
	        String varName = Utils.getVariableName(conf);
	        if ( !variable.getName().equals(varName) && !varName.equals("") )  {
	          //LOG.info(variable.getName() + " != " + varName );
	          continue;
	        } else {
	          LOG.info(variable.getName() + " == " + varName);
	          retVar = variable;
	        }
	
	      }
	
	    } catch (Exception e) {
	      System.out.println("Caught an exception in ArrayBasedFileInputFormat.getVariable()" + 
	                         e.toString() );
	    }
	
	    return retVar;
    }
 
  /**
   * Order a set of files and validate that they are valid files for this
   * program.
   * @param files List of FileStatus objects that represent the files 
   * in the input space
   * @param conf Configuration object for the currently executing MR program
   * @return A list of SHFileStatus objects representing the order the files
   * should have splits generated in
   */
  public List<SHFileStatus> orderMultiFileInput( List<FileStatus> files,
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

  @Override
  /**
   * this over rides the getSplits method in ArrayBasedFileInputFormat.
   * This is the method that actually returns the splits to be scheduled and 
   * executed by Hadoop.
   * @param job The JobContext object for the currently executing job
   * @return A List of InputSplit objects to be scheduled and run by Hadoop
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    List<SHFileStatus> shFiles = new ArrayList<SHFileStatus>();

    HashMap<Long, ArrayList<ArraySpec>> blockToSlab =
      new HashMap<Long, ArrayList<ArraySpec>>();

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

    PartMode partMode = Utils.getPartMode(job.getConfiguration());
    PlacementMode placementMode = 
        Utils.getPlacementMode(job.getConfiguration());

    /*
    if (Utils.getMultiFileMode(job.getConfiguration()) == MultiFileMode.concat) {
      orderMultiFileInput( files, shFiles);
    }
    */

    // set the starting offset for each file (depends on damasc.multi_file_mode 
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
                       "\tvar: " + spec.getVarName()); 
          writer.write( "\tcorner: " + Utils.arrayToString( spec.getCorner())); 
          writer.write( "\t shape: " + Utils.arrayToString( spec.getShape())); 
          writer.write("\t startOffset: " + Utils.arrayToString( spec.getLogicalStartOffset()) );
          writer.newLine();
        }
        i++;
      }
      writer.close();
    } else {
      LOG.info("No debugFileName set");
    }

    return splits;
  }

  @Override
  /**
   * Creates a RecordReader for NetCDF files
   * @param split The split that this record will be processing
   * @param context A TaskAttemptContext for the task that will be using
   * the returned RecordReader
   * @return A NetCDFRecordReacer 
   */
  public RecordReader<ArraySpec, Array> createRecordReader( InputSplit split, 
                                                TaskAttemptContext context )
                                                throws IOException { 
    NetCDFRecordReader reader = 
        new NetCDFRecordReader();
    reader.initialize( (ArrayBasedFileSplit) split, context);

    return reader;
  }

}
