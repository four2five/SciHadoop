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
 * This class is meant to represent splits for Array-based files. 
 */
public class ArrayBasedFileSplit extends InputSplit implements Writable {

  private Path _path = null;  // path of the in HDFS

  // Array of host names that this split *should* be assigned to 
  private String[] _hosts = null; 

  private long _totalDataElements = 0; // total number of cells represented by this split
  private ArrayList<ArraySpec> _arraySpecList;  // List of ArraySpecs for this split

  private static final Log LOG = LogFactory.getLog(ArrayBasedFileSplit.class);

  public ArrayBasedFileSplit() {}

  /**
  * NOTE: start and shape must be the same length
  *
  * Constructor for an ArrayBasedFileSplit
  * @param file the file name
  * @param variable the name of the variable this data belongs to
  * @param shape an n-dimensional array representing the length, in each dimension, of this array
  * @param dimToIterateOver which dimension to iterate over
  * @param startStep which step in the dimension being iterated over to start on
  * @param numSteps how many steps this split represents 
  * @param hosts the list of hosts containing this block, possibly null
  */
  public ArrayBasedFileSplit( Path path, ArrayList<ArraySpec> arraySpecList, 
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
   * Returns the path for the file this Spec corresponds to
   * @return path of the file corresponding to this Spec
   */
  public Path getPath() {
    return this._path;
  }

  /** 
   * Returns the ArrayList<ArraySpec> embedded within this split 
   * @return ArrayList<ArraySpec> with specific ArraySpecs 
   */
  public ArrayList<ArraySpec> getArraySpecList() {
    return this._arraySpecList;
  }

  @Override
  /**
   * Returns the total number of cells represented by the group of 
   * ArraySpec entries in this split
   * @return total number of cells represented by this split
   */
  public long getLength() throws IOException, InterruptedException {
    return this._totalDataElements;
  }

  @Override
  /**
   * Returns a list of hosts that locally possess a copy of the file system
   * block that this Split corresponds to
   * @return array of hostnames as Strings
   */
  public String[] getLocations() throws IOException, InterruptedException {
    if (null == this._hosts)
      return new String[]{};
    else 
      return this._hosts;
  }

  /**
   * Serializes this Split to the DataOutput object
   * @param out DataOutput object to serialize this Split to
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
   * Populates a Split via the data read from the DataInput object
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
   * Compares this object to another and returns a negative value,
   * zero or a positive value if this object is less than, equal to
   * or greater than the object passed in.
   * @param o the Object to compare this Split to. Assumed to bed another 
   * ArrayBasedFileSplit
   * @return a negative value, zero or positive value if this object 
   * is less than, equal to or greater than, respectively, the object
   * passed in
   */
  public int compareTo(Object o) {
    ArrayBasedFileSplit temp = (ArrayBasedFileSplit)o;
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
   * Handy function to print out the contents of this split
   * @return string versions of the data contained in this split
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
