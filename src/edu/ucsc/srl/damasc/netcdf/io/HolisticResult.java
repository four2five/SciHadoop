package edu.ucsc.srl.damasc.netcdf.io;

import java.util.Arrays;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

/**
 * This class stores the (potentially partial) result
 * of a holistic computation (like median). 
 */
public class HolisticResult implements Writable { 
  private int[] _values = null; // the values needed to apply the holisitic function
  private int _currentValueCount = 0; // how many values are currently set
  private boolean _full = false;
  private boolean _final = false;

  /**
   * Empty constructor
   */
  public HolisticResult() {
    this._values = new int[1];
    this._currentValueCount = 0;
    this._full = false;
    this._final = false;
  }

  private static final Log LOG = LogFactory.getLog(HolisticResult.class);

  /** 
   * Constructor used to do fast creation
   * @param value a single value to integrate into this partial result
   */
  public HolisticResult(int value ) throws Exception {
    this._values = new int[1];
    this._currentValueCount = 0;
    this.setValue(value);
    this._full = false;
    this._final = false;
  }

  /** 
   * Constructor
   * @param value a single value to integrate into this partial result
   * @param neededValueCount how many values are needed to generate a 
   * definitive result
   */
  public HolisticResult(int value, int neededValueCount) throws Exception {
    this._values = new int[neededValueCount];
    this._currentValueCount = 0;
    this.setValue(value);
    this._full = false;
    this._final = false;
  }

  /**
   * Returns the array of values seen for this data set so far
   * @return the array of values seen so far (by this object)
   */
  public int[] getValues() {
    return this._values;
  }

  /**
   * Returns a particular value that has been seen by this result object
   * @param index the array element to return
   * @return the value at the indicated array index location
   */
  public int getValue( int index ) {
    return this._values[index];
  }

  /**
   * Returns the number of values seen so far by this result object
   * @return number of values integrated into this result object so far
   */
  public int getCurrentValueCount() {
    return this._currentValueCount;
  }

  /**
   * Sorts the values stored in this object by their value
   */
  public void sort() {
    Arrays.sort(this._values);
  }

  /**
   * Resets this object for possible reuse
   */
  public void clear() {
    this._currentValueCount = 0;
    this._full = false;
    this._final = false;
  }

  /**
   * Returns how many values are needed to for this result object to calculate
   * its definitive result
   * @return the number of values needed for this object to generate its 
   * definitive result
   */
  public int getNeededValueCount() {
    return this._values.length;
  }

  /**
   * Final means that this object has received the appropriate number of 
   * results to generate its final answer (and has done so)
   * @return a boolean indicating whether this object has determined its
   * final result. If true, the values array has a single value which is 
   * the result for this data set. 
   */
  public boolean isFinal() {
    return this._final;
  }

  /**
   * Sets the "full" status, which means that the required number of values
   * required to calculate a result have been received by this object
   * @param isFull the value to use for whether this objects value array
   * has a sufficient number of values to calculate a result
   */
  private void setFull(boolean isFull) {
    this._full = isFull;
  }
  
  /**
   * Sets the "final" boolean, which indicates if this object has calculated
   * its final answer.
   * @param isFinal whether this object has calculated its answer
   */
  private void setFinal( boolean isFinal) {
    this._final = isFinal;
  }

  /**
   * Sets how many values are required for this object to generate its result
   * @param neededValueCount how many values are needed for this object to 
   * have its final result calculated
   */
  public void setNeededValueCount(int neededValueCount) {
    int[] newArray = new int[neededValueCount];
    for ( int i=0; i<this._currentValueCount; i++) {
      newArray[i] = this.getValue(i);
    }

    this._values = newArray;
  }
  
  /**
   * Returns whether this object has received its required number of 
   * inputs
   * @return whether this object has collected enough values for its result
   * to be calculated
   */
  public boolean isFull() {
    if ( this._values.length > 1 && 
         (this._values.length == this._currentValueCount)  ) {
      this._full = true;
    } 

    return this._full;
  }

  /**
   * Adds a value to this result object
   * @param value the value to add to this object
   */
  public void setValue(int value) throws IOException {
    if( this._values.length > 1 && this.isFull()) {
      throw new IOException("ERROR: adding an element to an already " + 
                            "full HolisticResult object." +
                            "Length: " + this._values.length);
    }

    if ( this._final == true ) {
      throw new IOException("ERROR: adding a value to a " +
                            "HolisticResult that has been marked final");
    }

    this._values[this._currentValueCount] = value;
    this._currentValueCount++;
  }

  /**
   * This means that the result for this object
   * has been calculated. This generates a new array, holding only the 
   * result, and sets the "final" status to true. 
   * @param value the result for this result object
   */
  public void setFinal( int value ) throws IOException {
    this._values = new int[1]; // free up the now useless ._values array
    this._currentValueCount = 0;
    this.setValue(value);
    this._final = true;
  }
    

  /**
   * Returns the contents of this result object in String form
   * @return the contents of this object as a String
   */
  public String toString() {
    return "values = " + Utils.arrayToString(this._values); 
  }

  /**
   * Initializes this object to having a single value in it.
   * This is used to reset a result object
   * @param value the single value to seed this result object with
   */
  public void setHolisticResult( int value ) throws IOException {
    this._values = new int[1];
    this._currentValueCount = 0;
    this.setValue(value);
  }

  /**
   * Initializes a result object with an array of results to 
   * add in to this object. 
   * TODO: optimize this by allocating _values here
   * @param values the array of values to add to this object
   */
  public void setHolisticResult( int[] values ) throws IOException {
        
    for ( int i=0; i<values.length; i++) {
      this.setValue(values[i]);
    }
  }

  /**
   * Initializes a result object with a single value and the count
   * of how mamy values are needed to calculate the result
   * @param value the single value to seed this result object with
   * @param neededValueCount how many values are needed to calculate
   * the result
   */
  public void setHolisticResult( int value, int neededValueCount) 
                                 throws IOException {
    this._values = new int[neededValueCount];
    this._currentValueCount = 0;
    this.setValue(value);

  }

  /**
   * Initializes a result object with another result object
   * @param result the HolisticResult object to use to initialize this
   * object
   */
  public void setHolisticResult( HolisticResult result) throws IOException {
    this.setHolisticResult( result.getValues());
  }

  /**
   * Merges a HolisticResult object into this HolisticResult object
   * @param result the HolisiticResult object to merge into this object
   */
  public void merge( HolisticResult result ) {
    try { 
      for ( int i=0; i<result.getCurrentValueCount(); i++) {
        this.setValue(result.getValues()[i]);    
      }
    } catch (IOException ioe){
      System.out.println("Caught an ioe in merge: \n ioe.toString() ");
    } catch (ArrayIndexOutOfBoundsException aiobe) {
      System.out.println("Caught an array bounds exception." + 
                         "Array length: " + this._values.length );
    }
  }

  /**
   * Serialize this object to a DataOutput object
   * @param out the DataOutput object to serialize this object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this._values.length);
    out.writeInt(this._currentValueCount);

    for( int i=0; i<this._currentValueCount; i++){
      out.writeInt(this._values[i]);
    }
    // these need to be sent last, so that we don't mark
    // something final and then add the one value to it
    out.writeBoolean(this._full);
    out.writeBoolean(this._final);
  }

  /**
   * Populate this object with data from a DataInput object
   * @param in the DataInput object to read data from when populating
   * this object
   */
  @Override
  public void readFields(DataInput in) throws IOException {

    // this may not be necessary
    this.setFull(false);
    this.setFinal(false);

    int localArraySize = in.readInt();
    this._values = new int[localArraySize];

    this._currentValueCount = 0;

    int toRead = in.readInt();

    for( int i=0; i<toRead; i++) {
      this.setValue(in.readInt());
    }

    this.setFull(in.readBoolean());
    this.setFinal(in.readBoolean());
  }

}
