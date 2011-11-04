package edu.ucsc.srl.damasc.netcdf.io.input;

import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.ucsc.srl.damasc.netcdf.io.NcHdfsRaf;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.io.input.ArrayBasedFileSplit;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.ma2.Array;
import ucar.nc2.Dimension;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;

/**
 * NetCDF specific code for reading data from NetCDF files.
 * This class is used by Map tasks to read the data assigned to them
 * from NetCDF files. 
 * TODO: don't copy data out of an InputSplit and the store it internally.
 * Rather, keep the split around and just access the data as needed
 */
public class NetCDFRecordReader 
      extends RecordReader<edu.ucsc.srl.damasc.netcdf.io.ArraySpec, Array> {

  private static final Log LOG = LogFactory.getLog(NetCDFRecordReader.class);
  private long _timer;
  private int _numArraySpecs;

  //this will cause the library to use its default size
  private int _bufferSize = -1; 

  private NetcdfFile _ncfile = null;
  private NcHdfsRaf _raf = null;
  private Variable _curVar; // actual Variable object
  private String _curVarName; // name of the current variable that is open
  private String _curFileName;

  // how many data elements were read the last step 
  private long _totalDataElements = 1; 

  // how many data elements have been read so far (used to track work done)
  private long _elementsSeenSoFar = 0; 

  private ArrayList<ArraySpec> _arraySpecArrayList = null;

  private ArraySpec _currentArraySpec = null; // this also serves as key
  private Array _value = null;
  private int _currentArraySpecIndex = 0;

  /**
   * Resets a RecordReader each time it is passed a new InputSplit to read
   * @param genericSplit an InputSplit (really an ArrayBasedFileSplit) that
   * needs its data read
   * @param context TaskAttemptContext for the currently executing progrma
  */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) 
                         throws IOException {
      
    this._timer = System.currentTimeMillis();
    ArrayBasedFileSplit split = (ArrayBasedFileSplit)genericSplit;
    this._numArraySpecs = split.getArraySpecList().size();
    Configuration job = context.getConfiguration();

    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(job);
        
    this._arraySpecArrayList = split.getArraySpecList();

    // calculate the total data elements in this split
    this._totalDataElements = 0;

    for ( int j=0; j < this._arraySpecArrayList.size(); j++) {
      this._totalDataElements += this._arraySpecArrayList.get(j).getSize();
    }

    // get the buffer size
    this._bufferSize = Utils.getBufferSize(job);

    this._raf = new NcHdfsRaf(fs.getFileStatus(path), job, this._bufferSize);
    this._ncfile = NetcdfFile.open(this._raf, path.toString());
        
  }

  /** this is called to load the next key/value in. The actual data is retrieved
   * via getCurrent[Key|Value] calls
   * @return a boolean that is true if there is more data to be read, 
   * false otherwise
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if ( !this._arraySpecArrayList.isEmpty() ) {
      // set the current element
      this._currentArraySpec = this._arraySpecArrayList.get(0);

      // then delete it from the ArrayList
      this._arraySpecArrayList.remove(0);

      // fixing an entirely random bug -jbuck TODO FIXME
      if ( this._currentArraySpec.getCorner().length <= 1 ) {
        return this.nextKeyValue();
      }

      // transfer the data
      loadDataFromFile(); 

      return true;
    } else {
      this._timer = System.currentTimeMillis() - this._timer;
      LOG.debug("from init() to nextKeyValue() returning false, " +
                "this record reader took: " + this._timer + 
                " ms. It had " + this._numArraySpecs + 
                " ArraySpecs to process" );

      return false;
    }
  }

  /**
   * Load data into the value element from disk.
   * Currently this only supports IntWritable. Extend this 
   * to support other data types TODO
   */
  private void loadDataFromFile() throws IOException {
    try { 

      // reuse the open variable if it's the correct one
      if ( this._curVarName == null ||  
          0 != (this._currentArraySpec.getVarName()).compareTo(this._curVarName)){
        LOG.debug("calling getVar on " + this._currentArraySpec.getVarName() );
        this._curVar = 
            this._ncfile.findVariable(this._currentArraySpec.getVarName());
      }
            

      if ( this._curVar ==null ) {
        LOG.warn("this._curVar is null. BAD NEWS");
        LOG.warn( "file: " + this._currentArraySpec.getFileName() + 
            "corner: " +   
            Arrays.toString(this._currentArraySpec.getCorner() ) + 
            " shape: " + Arrays.toString(this._currentArraySpec.getShape() ) );
      }

      LOG.warn( " File: " + this._currentArraySpec.getFileName() + 
                " startOffset: " + Utils.arrayToString(this._currentArraySpec.getLogicalStartOffset()) + 
               "corner: " + 
               Arrays.toString(this._currentArraySpec.getCorner()) + 
               " shape: " + 
               Arrays.toString(this._currentArraySpec.getShape()));

      // this next bit is to be able to set the dimensions of the variable
      // for this ArraySpec. Needed for flattening the groupID to a long
      ArrayList<Dimension> varDims = 
          new ArrayList<Dimension>(this._curVar.getDimensions());
      int[] varDimLengths = new int[varDims.size()];

      for( int i=0; i<varDims.size(); i++) {
        varDimLengths[i] = varDims.get(i).getLength();
      }
                
      this._currentArraySpec.setVariableShape(varDimLengths);
            
      long timerA = System.currentTimeMillis();
      this._value = this._curVar.read( this._currentArraySpec.getCorner(), 
                                       this._currentArraySpec.getShape()
                                     );
      long timerB = System.currentTimeMillis();
      LOG.info("IO time: " + (timerB - timerA) + " for " + 
               this._value.getSizeBytes() + " bytes");
    } catch (InvalidRangeException ire) {
    // convert the InvalidRangeException (netcdf specific) 
    // to an IOException (more general)
      throw new IOException("InvalidRangeException caught in " + 
                            "NetCDFRecordReader.loadDataFromFile()" + 
                            "corner: " + 
                           Arrays.toString(this._currentArraySpec.getCorner()) +
                            " shape: " + 
                            Arrays.toString(this._currentArraySpec.getShape()));
    }
  }
  
  /**  
   * Update _elementsSeenSoFar so that getProgress() is correct(-ish).
   * This is used to track task (and job) progress.
   */
  private void updateProgress() {
    this._elementsSeenSoFar += this._currentArraySpec.getSize();
  }

  /**
   * Returns the current key
   * @return An ArraySpec object indicating the current key
   */
  @Override
  public ArraySpec getCurrentKey() {
    // updates the counter for work done so far
    updateProgress();

    // extract the cell coordinate pointed to by the current key
    return this._currentArraySpec;
  }

  /**
   * return the current Value, in the Key/Value sense.
   * @return an Array object containing the values that correspond 
   * to the ArraySpec that is the current Key
   */
  @Override
  public Array getCurrentValue() {
    return this._value;
  }

  /**
   * Returns this jobs progress to the JobTracker so that the GUIs
   * can be updated.
   * @return a float representing the percent of this job that is
   * compeleted (between 0 and 1)
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)(this._elementsSeenSoFar / this._totalDataElements);
  }

  /**
   * Close files that were opened by the Record Reader and do general cleanup
   */
  @Override
  public void close() throws IOException {
    try {
      if (this._ncfile != null) {
        this._ncfile.close();
      }
    } catch (IOException ioe) {
      LOG.warn("ioe thrown in NetCDFRecordReader.close()\n");
    }
  }
}
