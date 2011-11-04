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
 * This class represents a result for a distributive function
 */
public class Result implements WritableComparable {
    private String _name;   // the variable name this data came from
    //private int[] _id;  // the corner of a given extraction shape 
    private long _id;   // the coordinate of the result, stored in flattened form
    private int _value; // the value found at this cell

    private static final Log LOG = LogFactory.getLog(Result.class);

    /**
     * Default constructor 
    */
    public Result() {}
    
    /**
     * Constructor  that allows an initial value to be set
     * @param seedValue an initial value to seed this result object with
     */
    public Result(int seedValue)  {
        this._value = seedValue; 
        this._name = "";
    }
    
    /**
     * Constructor  that allows a variable name, an ID, a variable shape
     * and an initial value to be set
     * @param name name of the variable this data came from
     * @param id the key for this result object
     * @param variableShape the shape of the variable that this result 
     * object belongs to, as an n-dimensional array
     * @param seedValue an initial value to seed this result object with
     */
    public Result(String name, int[] id, int[] variableShape, 
                  int seedValue) throws Exception {

        this._id = Utils.flatten(variableShape, id);

        this._name = new String(name);

        this._value = seedValue;
    }

    /**
     * Returns the ID for this result object
     * @return the ID for this result object as a long (flattened n-dimensional 
     * value)
     */
    public long getID() {
        return this._id;
    }

    /**
     * Returns this objects ID as an n-dimensional array
     * @param variableShape the shape to use to unflatten the ID 
     * @return ID for this object, as an n-dimensional array
    */
    public int[] getID(int[] variableShape) throws IOException {
        return Utils.inflate(variableShape, this._id);
    }

    /**
     * Returns the name of the variable that this object belongs to
     * @return name of the variable that this object belongs to
     */
    public String getName() {
        return this._name;
    }

    /**
     * Returns the current result for this result object
     * @return current result for this object
     */
    public int getValue() {
        return this._value;
    }

    /**
     * Sets the current result for this result object
     * @param value the value to use as the current result for this object
     */
    public void setValue(int value) {
        this._value = value;
    }

    /**
     * Sets the ID for this result object
     * @param newID the ID that this group Object should use
     * @param variableShape the shape of the variable that this result object
     * belongs to
     */
    public void setID( int[] newID, int[] variableShape) throws IOException {
        this._id = Utils.flatten(variableShape, newID);
    }

    /**
     * Sets the name of the variable that this result object belongs to
     * @param newName the name of the variable that this object belongs to
     */
    public void setName( String newName ) {
        this._name = newName;
    }

    /**
     * Returns the contents of this object as a string. The id is a single
     * long value.
     * @return contents of this object as a string
     */
    public String toString() {
        return _name + ": ID = " + this._id + " value = " + this._value; 
    }

    /**
     * Returns the contents of this object as a string. The ID is translated
     * into an n-dimensional array.
     * @param variableShape shape of the variable this result object belongs ot.
     * This is used to translate the ID to an n-dimensional array
     */
    public String toString(int[] variableShape) throws IOException {
        return _name + ": ID = " + Arrays.toString(Utils.inflate(variableShape, this._id)) + " value = " + this._value; 
    }

    /**
     * Sets the fields of this result object
     * @param varName the name of the variable that this result object belongs
     * to
     * @param id the ID for this result object
     * @param variableShape the shape of the variable that this result object
     * belongs to
     * @param value the current result that this object should use
     */
    public void setResult( String varName, int[] id, int[] variableShape, int value) throws IOException {
        this._name = new String(varName);
        this._id = Utils.flatten(variableShape, id);
        this._value = value;
    }

    /**
     * Sets the fields of this result object
     * @param varName the name of the variable that this result object belongs
     * to
     * @param id the ID for this result object
     * @param value current result that this object should use
     */
    public void setResult( String varName, long id, int value) {
        this._name = new String(varName);
        this._id = id;
        this._value = value;
    }

    /**
     * Sets the fields on this result object from another result object
     * @param result the Result object to pull data from
     */
    public void setResult( Result result) {
        this.setResult( result.getName(), result.getID(), result.getValue() );
    }

    /**
     * Compares this Result object to another Result object. 
     * @return Returns 
     * a value that is less than, equal to, or greater than zero if the 
     * object passed in is, respectively, less than, equal to, or greater 
     * than this object
     */
    public int compareTo(Object o) {
        int retVal = 0;
        Result other = (Result)o;

        retVal = (int)(this.getID() - other.getID());
        if ( 0 != retVal ) {
            return retVal;
        }

        return retVal;
    }

    /**
     * Serializes the output for this Result object to a DataOutput object
     * @param out the DataOutput object to write data to
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this._name);

        out.writeLong(this._id);

        out.writeInt(this._value);

    }

    /**
     * Populate this Result object with data read from a DataInput object
     * @param in the DataInput object to read data from
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this._name = Text.readString(in);
       
        this._id = in.readLong();

        this._value = in.readInt();
        
    }
}
