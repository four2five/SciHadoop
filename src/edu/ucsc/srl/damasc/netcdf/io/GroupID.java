package edu.ucsc.srl.damasc.netcdf.io;

import java.util.Arrays;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.Utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

/**
 * Stores the instance of the extraction shape that corresponds to an
 * array. Serves as a key for the shuffle phase of MapReduce jobs. 
 */
public class GroupID implements WritableComparable {
  private String _name;   // the variable name this data came from
  private int[] _groupID;  // the corner of a given extraction shape 

  public GroupID() {}

  private static final Log LOG = LogFactory.getLog(GroupID.class);

   
  /**
   * Constructor that sets the GroupID and the name of the variable
   * the ID corresponds to
   * @param groupID GroupID, as an n-dimensional variable
   * @param name Variable name that this GroupID belongs to
   */ 
  public GroupID(int[] groupID, String name) throws Exception {

    this._groupID = new int[groupID.length];
    for (int i=0; i < groupID.length; i++) {
      this._groupID[i] = groupID[i];
    }

    this._name = new String(name);
  }

  /**
   * Return the number of dimensions for this GroupID
   * @return the number of dimensions for the variable
   * that this GroupID belongs to
   */
  public int getRank() {
    return this._groupID.length;
  }

  /**
   * Returns the GroupID 
   * @return The GroupID as an n-dimensional array
   */
  public int[] getGroupID() {
    return this._groupID;
  }

  /**
   * Returns the name of the variable that this GroupID corresponds to
   * @return variable name as a String
   */
  public String getName() {
    return this._name;
  }

  /**
   * Sets the group ID for this GroupID object
   * @param newGroupID the ID for this object
   */
  public void setGroupID( int[] newGroupID) {
    this._groupID = newGroupID;
  }

  /**
   * Makes it possible to set the ID for a specific dimension
   * @param dim The dimension to set
   * @param val The value to set indicated dimension to
   */
  public void setDimension( int dim, int val) 
        throws ArrayIndexOutOfBoundsException {
    if ( dim < 0 || dim >= this._groupID.length)
      throw new ArrayIndexOutOfBoundsException("setDimension called with " +
          "dimension " + dim + " on groupID with dimensions " + 
          this._groupID.length);

    this._groupID[dim] = val;
  }

  /**
   * Sets the variable name for this GroupID object
   * @param newName the name of the Variable for this object
   */
  public void setName( String newName ) {
    this._name = newName;
  }

  /**
   * Returns the contents of this GroupID object as a String
   * @return a String version of this GroupID object
   */
  public String toString() {
    return _name + ": groupID = " + Arrays.toString(this._groupID); 
  }

  /**
   * Projects this GroupID from the local logical space
   * into the global logical space via the extraction shape
   * @param exShape The extraction shape to use to project this
   * GroupID into the global space
   * @return the group ID for this object, in the global logical space 
   */
  public String toString(int[] exShape) {
    int[] tempArray = new int[this._groupID.length];
    for ( int i=0; i<tempArray.length; i++) {
      tempArray[i] = this._groupID[i] * exShape[i];
    }

    return _name + ": groupID = " + Arrays.toString(tempArray); 
  }

  /**
   * Compares this GroupID to another GroupID
   * @param o a GroupID object to compare to this object
   * @return an int that is less than, equal to, or greater than zero
   * if the object passed in is less than, equal to, or greater than  
   * this GroupID, respectively
   */
  public int compareTo(Object o) {
    int retVal = 0;
    GroupID other = (GroupID)o;

    retVal = this.getRank() - other.getRank();
    if ( 0 != retVal ) 
      return retVal;

    for ( int i = 0; i < this._groupID.length; i++) {
      retVal = this._groupID[i] - other.getGroupID()[i];

      if (retVal != 0) {
        return retVal;
      }
    }

    return retVal;
  }

  /**
   * Serialize this object to a DataOutput object
   * @param out the DataOutput object to write the contents 
   * of this GroupID object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this._name);

    out.writeInt(this._groupID.length);
    for (int i = 0; i < this._groupID.length; i++)
      out.writeInt(this._groupID[i]);
  }

  /**
   * Populate this GroupID object from data read from a 
   * DataInput object
   * @param in the DataInput object to use to populate this
   * object from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    _name = Text.readString(in);
       
    int len = in.readInt();
    this._groupID = new int[len];
    for (int i = 0; i < this._groupID.length; i++)
      this._groupID[i] = in.readInt();
        
  }

  /**
   * Calculates the length, on each dimension, that is needed,
   * in terms of cells, to iterate by 1 on that dimension
   * @param shape The shape of the logical space to generate
   * a stride array for
   * @return an array of longs with the stride length for each
   * dimension
   */
  private long[] computeStrides(int[] shape) throws IOException {
    long[] stride = new long[shape.length];
    long product = 1;
    for (int i = shape.length - 1; i >= 0; i--) {
      final int dim = shape[i];
      if (dim < 0)
        throw new IOException("Negative array size");
      stride[i] = product;
      product *= dim;
    }
    return stride;
  }

  /**
   * Takes a variable shape and uses that to translate the n-dimensional
   * group ID for this object into a single long value. This is used
   * to create the Key for routing data in Hadoop.
   * @param variableShape the shape to use when flattening the group ID
   * for this object. This should be the shape of the variable the GroupID 
   * is associated with.
   * @return a long value that represents this GroupID in a flattened 
   * logical space
   */
  public long flatten( int[] variableShape ) throws IOException {
    long[] strides = computeStrides( variableShape);
    long flattenedID = 0;

    for( int i = 0; i < this._groupID.length; i++) {
      flattenedID += ( (long)(this._groupID[i]) * (strides[i]) );
    }
        
    //debug only
    if ( flattenedID < 0){
      LOG.info(" fid: " + flattenedID + " vs: " + 
               Utils.arrayToString(variableShape) + " str: " + 
               Utils.arrayToString(strides) +
               " gid: " + Utils.arrayToString(this._groupID) );
    } 

    return flattenedID; 
  }

  /**
   * Translates a flattened GroupID into an n-dimensional location
   * @param variableShape the shape of the variable that was used
   * to flatten the GroupID initially
   * @param flattenedValue the flattened value to turn back into an 
   * n-dimensional shape
   * @return an n-dimensional array that is a group ID
   */
  public int[] unflatten( int[] variableShape, long flattenedValue ) 
                          throws IOException {
    long[] strides = computeStrides( variableShape);
    int[] results = new int[variableShape.length];

    for( int i = 0; i < variableShape.length; i++) {
      results[i] = (int) (flattenedValue / strides[i]);
      flattenedValue -= ((long)(results[i]) * strides[i]);
    }
        
    return results;
  }
}
