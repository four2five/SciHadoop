package edu.ucsc.srl.tools;

import java.util.Date;
import java.util.Random;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import ucar.nc2.NetcdfFileWriteable;

import ucar.nc2.Dimension;
import ucar.nc2.Variable;
import ucar.ma2.Array;
import ucar.ma2.ArrayObject;
import ucar.ma2.ArrayInt;
import ucar.ma2.ArrayLong;
import ucar.ma2.ArrayFloat;
import ucar.ma2.Index;
import ucar.ma2.IteratorFast;
import ucar.ma2.DataType; 
import ucar.ma2.InvalidRangeException;

import ucar.ma2.IndexIterator;

/**
 * This class generates a exemplary netcdf data set
 */
public class NetcdfFileGenerator {

    // keep the memory usage below this 
    // let's start with 64 megs

    private static int maxMemory = 67108864; // bytes

    private Dimension _unlimitedDim;

    public NetcdfFileGenerator() {
        this._unlimitedDim = null ;
    }

    private void writeCoordinateVariable( VariableEntry variable, 
                                          NetcdfFileWriteable ncFile ) 
                                          throws IOException, InvalidRangeException {

        int[] dimensions = {variable.getSize()};
        Array array;

        // so far, everything is a float or an int
        // way too much code duplication but I'm done fightin java for now
        if ( variable.getType() == DataType.INT ) {
            array = new ArrayInt(dimensions);
            int tempInt = 0;
            IndexIterator iter = array.getIndexIterator();

            while (iter.hasNext() ) {
                iter.getIntNext();
                iter.setIntCurrent(tempInt);
                tempInt++;
            }

            ncFile.write(variable.getVariableName(),array);
        } else if ( variable.getType() == DataType.FLOAT )  {
            array = new ArrayFloat(dimensions);
            float tempFloat = 0;
            IndexIterator iter = array.getIndexIterator();

            while (iter.hasNext() ) {
                iter.getFloatNext();
                iter.setFloatCurrent(tempFloat);
                tempFloat++;
            }

            ncFile.write(variable.getVariableName(),array);
        } else if ( variable.getType() == DataType.LONG)  {
            array = new ArrayLong(dimensions);
            long tempLong = 0;
            IndexIterator iter = array.getIndexIterator();

            while (iter.hasNext() ) {
                iter.getLongNext();
                iter.setLongCurrent(tempLong);
                tempLong++;
            }

            ncFile.write(variable.getVariableName(),array);
        }

        //ncFile.write(variable.getVariableName(),array);

    }

    private NetcdfFileWriteable writeNetcdfMetadata( NetcdfFileWriteable ncFile,
                                                     String variableName,
                                                     ArrayList<ArrayList<VariableEntry>> variableListList )
        throws IOException {

        int suffixInt = 1;

        for ( ArrayList<VariableEntry> variableList : variableListList )  {
            ArrayList<Dimension> dims = new ArrayList<Dimension>();

            // add coordinate variables for each dimension
            for ( int i=0; i<variableList.size(); i++ ) {
                VariableEntry temp = variableList.get(i);

                Dimension tempDim; 

                System.out.println("writing metadata for dim " + temp.getVariableName() );

                if ( temp.isUnlimited() ) { 
                    // see if there is already an unlimited dimension in this filea, 
                    // if so, use it. If not, add it 
                    //if ( ncFile.hasUnlimitedDimension() )  { 
                    if ( this._unlimitedDim == null )  { 
                        tempDim = ncFile.addUnlimitedDimension( temp.getVariableName() );
                        this._unlimitedDim = tempDim;
                    } else { 
                        //tempDim = this._unlimitedDim;
                        dims.add(this._unlimitedDim);
                        continue;
                    }

                } else {
                    tempDim = ncFile.addDimension(temp.getVariableName(), temp.getSize() );
                }

                // add a coordinate variable
                Dimension[] tempDimArray = {tempDim};
                ncFile.addVariable( temp.getVariableName(), temp.getType(), tempDimArray );
                ncFile.addVariableAttribute( temp.getVariableName(), "units", temp.getUnits() );
                ncFile.addVariableAttribute( temp.getVariableName(), "long_name", temp.getLongName() );
                dims.add(tempDim);
            }

            // add the main variable
            System.out.println("adding variable " + (variableName + Integer.toString(suffixInt) ) );
            ncFile.addVariable(variableName + Integer.toString(suffixInt), DataType.INT, dims);
            suffixInt++;
        }
/*
        ArrayList<Dimension> dims = new ArrayList<Dimension>();
        Dimension tempDim;
        String localName = "dim";

        for( int i = 0; i < dimensions.length; i++){
            if ( (0 == i) && variableDim) {
                tempDim = ncFile.addUnlimitedDimension("dim" + Integer.toString(i));
            } else {
                tempDim = ncFile.addDimension("dim" + Integer.toString(i), dimensions[i]);
            }
            dims.add( tempDim);
        }
        
        ncFile.addVariable(varName, dataType, dims);
*/

        return ncFile;
    }

    // this is the highest dimension that will be a one for writings sake
    private int determineHighestNonWriteDimension( int[] dimensions, long maxWrite ) {
       int firstStepDim = dimensions.length - 1;

        long totalStepSize = 1;

        for(int i = dimensions.length - 1; i >= 0; i--) {

            if ( (dimensions[i] * totalStepSize) < maxWrite ) {
                firstStepDim = i-1;
                totalStepSize *= dimensions[i];
            } else {
                break;
            }
        }

        return Math.max(firstStepDim,0);
    }

    private int[] createWriteStep(int[] dimensions, long maxWrite, long highestNonWriteDimension) {

        int[] returnArray = new int[dimensions.length];
        int i;
        for ( i=0; i<= highestNonWriteDimension; i++ ) {
            returnArray[i] = 1;
        }

        for (; i < returnArray.length; i++) {
            returnArray[i] = dimensions[i];
        }

        return returnArray;
    }

    public int determineNumberOfStepWrites(int[] stepSize, long writeSize) {
        long tempLong = 0;

        for (int i=0; i<stepSize.length; i++) {
            if (i == 0 )
                tempLong = stepSize[i];
            else 
                tempLong *= stepSize[i];
        }

        return (int) (writeSize / tempLong);
    }

    private int populateFile2( NetcdfFileWriteable ncFile, String varName,
                                              DataType dataType,
                                              int[] dimensions, int valueCounter,
                                              Random generator) 
        throws IOException, InvalidRangeException  {

        long singleWriteSize = maxMemory / dataType.getSize();
        int highestNonWriteDim = determineHighestNonWriteDimension( dimensions, singleWriteSize);        
        int[] singleStep = createWriteStep(dimensions, singleWriteSize, highestNonWriteDim);
        int numStepWrites = determineNumberOfStepWrites( singleStep, singleWriteSize);

        System.out.println("SingleWriteSize: " + singleWriteSize + " datatype size: " + dataType.getSize() + 
                           " singleStep: " + arrayToString(singleStep) + " numberStepsPerWrite: " + numStepWrites);

        int[] allOnes = new int[dimensions.length];
        int[] allZeros = new int[dimensions.length];

        for (int i=0; i<allOnes.length; i++) {
            allOnes[i] = 1;
            allZeros[i] = 0;
        }
       
        Index origin = new Index(dimensions, allOnes);

        long totalSize = origin.getSize();
        long writtenSoFar = 0;

        boolean done = false;

        while ( !done ) {
            for ( int i=0; i < numStepWrites; i++) {
                // this is a crack at an optimization
                                    
                if ( (highestNonWriteDim >= 0) && 
                     ((origin.getCurrentCounter()[highestNonWriteDim] + numStepWrites) <= dimensions[highestNonWriteDim])  
                   ) {
                    singleStep[highestNonWriteDim] = numStepWrites;
                    System.out.println("JUST OPTIMIZED. New write step: " + arrayToString(singleStep) );
                    // keep 'i' right
                    i += numStepWrites - 1;
                } else {
                    singleStep[highestNonWriteDim] = 1;
                }


                Array array = new ArrayInt( singleStep );
                IndexIterator iter = array.getIndexIterator();

                while( iter.hasNext() ) {
                    iter.getIntNext();
                    // uncomment the following line for a random distribution
                    //iter.setIntCurrent((int)  (Math.abs(generator.nextGaussian()) * 40) );
                    // uncomment this line for an incrementing value
                    iter.setIntCurrent(valueCounter);
                    valueCounter++;

                  // book keeping
                    writtenSoFar++;
                    //origin.incr();
                }

                System.out.println("Writing to file: " + ncFile.getLocation() + ". var_name: " + varName  + 
                                   " origin: " + arrayToString(origin.getCurrentCounter()) + 
                                   " writeSize: " + array.getSize() + 
                                   " write shape: " + arrayToString(singleStep) );

                // write to the actual file
                ncFile.write(varName, origin.getCurrentCounter(), array);

                // update origin accordingly
                
                for( int j=0; j<Index.computeSize(singleStep); j++) {
                    //writtenSoFar++;
                    origin.incr();
                }
                
                
                System.out.println("\tcurrentIndex: " + arrayToString(origin.getCurrentCounter()) + 
                                   " currentElement: " + origin.currentElement() + " totalsize: " + (totalSize) + 
                                   " writtenSoFar: " + writtenSoFar );
                
                if ( writtenSoFar  >= totalSize ) {
                    done = true;    
                    break;
                }
            }
        }

        return valueCounter;
    }

    // return the last value written to the file
    private int createFile( String variableName, DataType dataType, int writeSeed ){

        // hacky init
        this._unlimitedDim = null;

        ArrayList<ArrayList<VariableEntry>> variableListList = new ArrayList<ArrayList<VariableEntry>>();

        // seperate method to define the variables for this file
        /*
        ArrayList<VariableEntry> variableList3 = defineDataForThisFile3();
        variableListList.add(variableList3);

        ArrayList<VariableEntry> variableList1 = defineDataForThisFile1();
        variableListList.add(variableList1);
        */

        ArrayList<VariableEntry> variableList2 = defineDataForThisFile2();
        variableListList.add(variableList2);

        Date now = new Date();
        Random generator = new Random( now.getTime() );
        int valueCounter = writeSeed;

        String filename = Long.toString(now.getTime()) + ".nc";

        try { 
            // create the file
            NetcdfFileWriteable ncfile = NetcdfFileWriteable.createNew(filename, false);
   
            // this loop needs to define all the meta-data prior to any data being written
            // set the metadata 
            System.out.println("\t calling writeNetcdfMetadata for file " + ncfile.getLocation() );
            ncfile = this.writeNetcdfMetadata(ncfile, variableName, variableListList);

            // this is only called once per file
            ncfile.create();

            int suffixInt = 1;
            for ( ArrayList<VariableEntry> variableList : variableListList ) { 
                // write out coordinate variables
                for (VariableEntry entry : variableList) { 
                    writeCoordinateVariable( entry, ncfile );
                }
                // pull out the dimensions of the variables from variableList
                int[] dimensions = new int[variableList.size()];
                for (int i=0; i<dimensions.length; i++) {
                    dimensions[i] = variableList.get(i).getSize();
                }

                // write data to the file
                valueCounter = this.populateFile2(ncfile, variableName + Integer.toString(suffixInt), 
                                                  dataType, dimensions, valueCounter, generator); 
                                                  
                suffixInt++;
            }   

            // close the file
            ncfile.close();
        } catch (IOException ioe){
            System.out.println("IOException: " + ioe.toString() );
        } catch (InvalidRangeException e) {
            System.out.println("InvalidRangeException: " + e.toString() );
        } 

        return valueCounter;
    }

    private String arrayToString( int[] array ) {
        String tempStr = "";

        for (int i=0; i<array.length; i++) {
            if (i>0)
                tempStr += ",";
            tempStr += array[i];
        }

        return tempStr;
    }

    // This is where the dimensions and shape of the file are configured
    public static void main(String args[]) {

        // list out the dimensions for this file

        String variableName = "windspeed"; // this is for the measurement actually in this data set
        DataType dataType = DataType.INT; // data type for the actual data

        NetcdfFileGenerator myGen = new NetcdfFileGenerator(); 
        int numFiles = 1;
        int writeSeed = 0;

        for ( int i = 0; i < numFiles; i++) { 
          writeSeed = myGen.createFile(variableName, dataType, writeSeed);
        }

    }
    
    private ArrayList<VariableEntry> defineDataForThisFile1 () { 
        ArrayList<VariableEntry> variableList = new ArrayList<VariableEntry>();

        variableList.add(new VariableEntry("time", 20, DataType.INT, true, 
                         "days", "time since midnight, 1,1,1980") );
        variableList.add(new VariableEntry("lat1", 720, DataType.FLOAT, false, 
                         "latitude", "half_degrees_from_north" ) );
        variableList.add(new VariableEntry("lon1", 720, DataType.FLOAT, false, 
                         "longitude", "quarter_degrees_going_east" ) );
        variableList.add(new VariableEntry("elev1", 100, DataType.FLOAT, false, 
                         "elevation", "meters") );

        return variableList;
    }

    /**
     * This will generate a data file that is 
     */
    private ArrayList<VariableEntry> defineDataForThisFile2() { 
        ArrayList<VariableEntry> variableList = new ArrayList<VariableEntry>();

        //variableList.add(new VariableEntry("time", 14600, DataType.INT, true, 
        variableList.add(new VariableEntry("time", 80, DataType.INT, true, 
                         "days", "time since midnight, 1,1,1970") );
        //variableList.add(new VariableEntry("lat2", 180, DataType.FLOAT, false, 
        variableList.add(new VariableEntry("lat2", 180, DataType.FLOAT, false, 
                         "latitude", "half_degrees_from_north" ) );
        variableList.add(new VariableEntry("lon2", 360, DataType.FLOAT, false, 
                         "longitude", "quarter_degrees_going_east" ) );
        //variableList.add(new VariableEntry("elev2", 35, DataType.FLOAT, false, 
        variableList.add(new VariableEntry("elev2", 50, DataType.FLOAT, false, 
                         "elevation", "meters") );

        return variableList;
    }

    // non-record dimension data
    private ArrayList<VariableEntry> defineDataForThisFile3() { 
        ArrayList<VariableEntry> variableList = new ArrayList<VariableEntry>();

        variableList.add(new VariableEntry("time3", 100, DataType.INT, false, 
                         "days", "time since midnight, 1,1,1980") );
        variableList.add(new VariableEntry("lat3", 180, DataType.FLOAT, false, 
                         "latitude", "half_degrees_from_north" ) );
        variableList.add(new VariableEntry("lon3", 180, DataType.FLOAT, false, 
                         "longitude", "quarter_degrees_going_east" ) );
        variableList.add(new VariableEntry("elev3", 50, DataType.FLOAT, false, 
                         "elevation", "meters") );

        return variableList;
    }

    public class VariableEntry {
        private String _variableName;
        private int _length;
        private DataType _type;
        private boolean _isUnlimited;
        private String _longName;
        private String _units;

        public VariableEntry( String variableName, int length, DataType type, boolean isUnlimited,
                              String longName, String units) {
            _variableName = variableName;
            _length = length;
            _type = type;
            _isUnlimited = isUnlimited;
            _longName = longName;
            _units = units;
        }

        public String getVariableName() { 
            return _variableName;
        }

        public int getSize() {
            return _length;
        }

        public DataType getType() {
            return _type;
        }

        public boolean isUnlimited() {
            return _isUnlimited;
        }

        public String getLongName() {
            return _longName;
        }

        public String getUnits() {
            return _units;
        }
    }

}
