package edu.ucsc.srl.damasc.netcdf.io.input;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;

/**
 * This class represents an InputSplit which corresponds to a 
 * NetCDF file.
 */
public class NetCDFFileSplit extends InputSplit implements Writable {

  private Path _path = null;
  private String[] _hosts = null;
  private long _totalDataElements = 0;
  private ArrayList<ArraySpec> _arraySpecList; 
  private static final Log LOG = LogFactory.getLog(NetCDFFileSplit.class);

  public NetCDFFileSplit() {}

  /**
  * NOTE: start and shape must be the same length
  *
  * @param file the file name
  * @param variable the name of the variable this data belongs to
  * @param shape an n-dimensional array representing the length, 
  * in each dimension, of this array
  * @param dimToIterateOver which dimension to iterate over
  * @param startStep which step in the dimension being iterated over to start on
  * @param numSteps how many steps this split represents 
  * @param hosts the list of hosts containing this block, possibly null
  */
  public NetCDFFileSplit( Path path, ArrayList<ArraySpec> arraySpecList, 
                          String[] hosts) {
    this._path = new Path(path.toString());
    this._arraySpecList = new ArrayList<ArraySpec>(arraySpecList);

    this._hosts = new String[hosts.length];
    for (int i=0; i<this._hosts.length; i++) {
      this._hosts[i] = hosts[i];
    } 

    for (int i=0; i<this._arraySpecList.size(); i++) {
      this._totalDataElements += this._arraySpecList.get(i).getSize();
    }
  }

  /**
   * Return the path for the file which this split corresponds to
   * @return Path a Path object representing a file in HDFS
   */
  public Path getPath() {
    return this._path;
  }

  /**
   * Returns the List of ArraySpec objects which makeup this InputSplit
   * @return an ArrayList of ArraySpec objects which represent the logical
   * space to be processed. 
   */
  public ArrayList<ArraySpec> getArraySpecList() {
    return this._arraySpecList;
  }

  /**
   * Returns the number of cells represented by the set of ArraySpecs
   * in this Split
   * @return the number of total cells represented by this split
   */
  @Override
  public long getLength() throws IOException, InterruptedException {
    return this._totalDataElements;
  }

  /**
   * Returns the list of hosts which contain, locally, the file system 
   * block that this InputSplit is assigned to
   * @return an array of Strings that are hostnames. The scheduler should attempt
   * to place this InputSplit on one of those hosts
   */
  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    if (null == this._hosts)
      return new String[]{};
    else 
      return this._hosts;
  }

  /**
   * Serialze this structure to a DataOutput object
   * @param out The DataOutput object to write the context of this split to
   */
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this._path.toString());

    // first, write the number of entires in the list
    out.writeInt(this._arraySpecList.size() );

    // then, for each entry, write it 
    for (int i=0; i<this._arraySpecList.size(); i++) {
      ArraySpec array = this._arraySpecList.get(i);
      array.write(out);
    }
  }

  /**
   * Populate a NetCDFFileSplit from the data read from the DataInput object
   * @param in DataInput object to read data from
   */
  public void readFields(DataInput in) throws IOException {
    this._path = new Path(Text.readString(in));

    // read in the list size
    int listSize = in.readInt();
    this._arraySpecList = new ArrayList<ArraySpec>(listSize);
       
    // for each element, create a corner and shape
    for (int i=0; i< listSize; i++) {
      ArraySpec array = new ArraySpec();
      array.readFields(in);
      this._arraySpecList.add(array);
    }
  }

  /**
   * Compares this NetCDFFileSplit to another.
   * @return an int that is less than, equal to, or greater than 0, 
   * depending on whether the object passed in is lesser than, equal to,
   * or greater than this NetCDFFileSplit (respectively)
   */
  public int compareTo(Object o) {
    NetCDFFileSplit temp = (NetCDFFileSplit)o;
    int retVal = 0;

    // we'll compare the first entry in the ArrayList<ArraySpec> 
    // from each object. If they are 
    // the same, then we'll assume these are the same element 
    // (since each ArraySpec should exist in one and
    // only one entry
    ArraySpec arrayA = this._arraySpecList.get(0);
    ArraySpec arrayB = temp.getArraySpecList().get(0);

    if (arrayA == null) {
      return -1;
    } else if ( arrayB == null) {
      return 1;
    }

    for (int i=0; i < arrayA.getCorner().length; i++) {
      retVal = new Integer(arrayA.getCorner()[i] - 
                           arrayB.getCorner()[i]).intValue();
      if (retVal != 0) {
        return retVal;
      }
    }

    return 0;
  }

  /**
   * Writes the contents of this NetCDFFileSplit to a string
   * @return a String representing the content of this NetCDFFileSplit
   */
  public String toString() {
    String tempStr = "";

    tempStr += "file: " + this._path.getName() + " with ArraySpecs:\n";

    for ( int i=0; i<this._arraySpecList.size(); i++) {
      tempStr += "\t" + this._arraySpecList.get(i).toString() + "\n";
    }

    return tempStr;
  }
}
