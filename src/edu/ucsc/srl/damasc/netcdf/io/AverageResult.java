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
 * This class represents the results of an average operation.
 * It tracks the results of average for part of the data and allows
 * for accurrate aggregation of partial results into the final result.
 */
public class AverageResult implements Writable { 
  private double _currentValue;
  private int _valuesCombinedCount;

  private static final Log LOG = LogFactory.getLog(AverageResult.class);

  /**
   * Constructor
   */
  public AverageResult() {
   // this._currentValue = 0;
    //this._valuesCombinedCount = 0;
    this.clear();
  }


  /**
   * Create an AverageResult object based on 
   * a single value.  the mapper uses this to do fast creation
   * @param value the single value to use to seed this AverageResult
   * object
   */
  public AverageResult(int value ) throws Exception {
    this._currentValue = (double)value;
    this._valuesCombinedCount = 1;
  }

  /**
   * Constructor that takes both a current value and a count of the values
   * that were combined to get that value.
   * @param value the current average value of the results aggregated
   * @param valudsCombinedCount the number of values that have been
   * combined so far
   */
  public AverageResult(int value, int valuesCombinedCount) throws Exception {
    this._currentValue = (double)value;
    this._valuesCombinedCount = valuesCombinedCount;
  }

  /**
   * Re-initialize this object for potential reuse
   */
  public void clear() {
    this._currentValue = (double)0;
    this._valuesCombinedCount = 0;
  }

  /**
   * Return the current average based on the values that have been
   * processed so far
   * @return the current average value
   */
  public int getCurrentValue() {
    return (int)this._currentValue;
  }

  /**
   * Return the current average value as a double
   * @return the current average value as a double
   */
  public double getCurrentValueDouble() {
    return this._currentValue;
  }

  /**
   * The number of values that have been added into the running 
   * average so far
   * @return the count of values that have been combined so far
   */
  public int getValuesCombinedCount() {
    return this._valuesCombinedCount;
  }

  /**
   * Adds a value to the running total. This will update both
   * the average value and the count of values that have been
   * processed so far
   * @param value the value to add to the running average
   */
  public void addValue(int value) throws IOException {
    //LOG.info("pre-add (int): value: " + this._currentValue + " count: " + this._valuesCombinedCount);
    this._currentValue = ((this._currentValue * this._valuesCombinedCount) + value ) / (this._valuesCombinedCount + 1);
    this._valuesCombinedCount++;
    //LOG.info("post-add (int): value: " + this._currentValue + " count: " + this._valuesCombinedCount);
  }

  /**
   * Merge this AverageResult object with another
   * @param result the AverageResult object to merge with this one
   */
  public void addAverageResult( AverageResult result ) { 
    //LOG.info("pre-add (AvgRes): value: " + this._currentValue + " count: " + this._valuesCombinedCount + 
    //         " in.value: " + result.getCurrentValueDouble() + " in.count: " + result.getValuesCombinedCount() );
    this._currentValue =  ((this._currentValue * this._valuesCombinedCount) + 
                          (result.getCurrentValueDouble() * result.getValuesCombinedCount())) 
                         / ((double)this._valuesCombinedCount + result.getValuesCombinedCount());
    this._valuesCombinedCount += result.getValuesCombinedCount();
    //LOG.info("post-add (AvgRes): value: " + this._currentValue + " count: " + this._valuesCombinedCount + 
    //         " in.value: " + result.getCurrentValueDouble() + " in.count: " + result.getValuesCombinedCount() );

  }

  /**
   * Set the current average value from an int
   * @param value the value to set as the current average value. 
   */
  protected void setCurrentValue( int value ) {
    this._currentValue = (double)value;
  }

  /**
   * Set the current value from a double
   * @param value the double value to use to set the current value
   */
  protected void setCurrentValue( double value ) {
    this._currentValue = value;
  }

  /**
   * Set the number of values combined to form the current
   * average value  
   * @param newCount the new number of values used to 
   * calculate the current average value
   */
  public void setValuesCombinedCount( int newCount ) {
    this._valuesCombinedCount = newCount;
  }

  /**
   * Set both the current average value and the count 
   * of the number of values processed to get to that average.
   * @param value the current average value
   * @param newCount how many values have been processed so far
   */
  protected void setValue( int value, int newCount) { 
    this.setCurrentValue( value);
    this.setValuesCombinedCount( newCount);
  }

  /**
   * Write the contents of this AverageResult object out to a String 
   * @return a String representation of this AverageResult object
   */ 
  public String toString() {
    return "value = " + this._currentValue + 
           " count = " + this._valuesCombinedCount;
  }

  /**
   * Serialize this AverageResult object to a DataOutput object
   * @param out the DataOutput object to write the contents of this
   * object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(this._currentValue);
    out.writeInt(this._valuesCombinedCount);
  }

  /**
   * Populate this AverageResult object from the data read from 
   * a DataInput object
   * @param in the DataInput object to read data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {

    this.setCurrentValue(in.readDouble());
    this.setValuesCombinedCount(in.readInt());
  }
}
