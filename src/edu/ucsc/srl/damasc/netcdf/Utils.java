package edu.ucsc.srl.damasc.netcdf;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.BlockLocation;

import edu.ucsc.srl.damasc.netcdf.io.GroupID;

/**
 * A collection of methods that are used in various parts of the code to read / write
 * global configuration data as well as some small helper functions
 */
public class Utils {

    private static final Log LOG = LogFactory.getLog(Utils.class);

    public static final String VARIABLE_NAME = "damasc.variable_name";
    public static final String SAMPLE_RATIO = "damasc.sample_ratio";
    public static final String VARIABLE_SHAPE_PREFIX = "damasc.variable_shapes";
    public static final String LOW_FILTER = "damasc.low_filter";
    public static final String HIGH_FILTER = "damasc.high_filter";
    //public static final String OPT_MODE = "damasc.optimization_mode";
    public static final String PART_MODE = "damasc.partition_mode";
    public static final String PLACEMENT_MODE = "damasc.placement_mode";
    public static final String NO_SCAN = "damasc.noscan";
    public static final String USE_COMBINER = "damasc.use_combiner";
    public static final String HOLISTIC = "damasc.holistic";
    public static final String QUERY_DEPENDANT = "damasc.query_dependant";
    public static final String EXTRACTION_SHAPE = "damasc.extraction_shape";
    public static final String NUMBER_REDUCERS = "damasc.number_reducers";
    public static final String OPERATOR = "damasc.operator";
    public static final String DEBUG_LOG_FILE = "damasc.logfile";
    public static final String BUFFER_SIZE = "damasc.buffer_size";
    public static final String MULTI_FILE_MODE = "damasc.multi_file_mode";
    public static final String FILE_NAME_ARRAY = "damasc.file_name_array";
    public static final String DEFAULT_BUFFER_SIZE = "8192";

    private static float sampleRatio;
    private static boolean sampleRatioSet = false;
    private static int[] variableShape;
    private static boolean variableShapeSet = false;
    private static int validLow;
    private static int validHigh;
    private static boolean validSet = false;
    private static Operator operator = Operator.optUnknown;
    private static PartMode partMode = PartMode.proportional;
    private static PlacementMode placementMode = PlacementMode.roundrobin;
    private static boolean operatorSet = false;
    private static boolean partModeSet = false;
    private static boolean placementModeSet = false;
    private static int[] extractionShape;
    private static boolean extractionShapeSet = false;
    private static boolean holisticEnabled = false;
    private static boolean holisticEnabledSet = false;
    private static boolean queryDependantEnabled = false;
    private static boolean queryDependantEnabledSet = false;
    private static boolean noScanEnabled = false;
    private static boolean noScanEnabledSet = false;
    private static boolean useCombiner = false;
    private static boolean useCombinerSet = false;
    private static int numberReducers = 1;
    private static boolean numberReducersSet = false;
    private static String debugLogFile = "";
    private static boolean debugLogFileSet = false;
    private static int bufferSize = -1;
    private static boolean bufferSizeSet = false;
    private static MultiFileMode multiFileMode = MultiFileMode.combine;
    private static boolean multiFileModeSet = false;


    public Utils() {
    }

    public static enum Operator { average, simpleMax, max, simpleMedian, median, nulltest, optUnknown }

    // partitioning scheme 
    public static enum PartMode{ proportional, record, calculated }

    public static enum PlacementMode{ roundrobin, sampling, implicit}

    public static enum MultiFileMode{ combine, concat}

    /**
     * Adds a file name to the fileNameArray variable in conf if it doesn't exist in there already
     * @param fileName name of the file to add to fileNameArray
     * @param conf Configuration object for the currently executing program
     * @return returns the index of the fileName we just added
     */
    public static int addFileName(String fileName, Configuration conf) {
      ArrayList<String> fileNameArray = new ArrayList<String>(conf.getStringCollection(FILE_NAME_ARRAY));
      fileNameArray.add(fileName);
      int retVal = fileNameArray.indexOf(fileName);

      conf.setStrings(FILE_NAME_ARRAY, stringArrayToString(fileNameArray));

      return retVal;
    }

    /**
     * Helper function that converts a Collection<String> into a single comma-delimited  String
     * @param strings a Collection of String objects
     * @return a single String containing all the entries in the Collection passed in
     */
    public static String stringArrayToString(Collection<String> strings) {
      String retString = "";
      int numElements = 0;
      for ( String s : strings ) {
        if ( numElements > 0) {
          retString += "," + s;
        } else {
          retString = s;
        } 
        numElements++;
      }

      return retString;
    }

    /**
     * Sets the Variable Shape for the current job. 
     * @param conf Configuration object for the current program
     * @param variableShape a comma delimited string representing the shape of the variable being
     * processed by the current job
     */
    public static void setVariableShape(Configuration conf, String variableShape) {
      conf.set( Utils.VARIABLE_SHAPE_PREFIX, variableShape);
      variableShapeSet = true;
    }

    /**
     * Get the shape of the Variable as an n-dimensional array
     * @param conf Configuration object for the current program
     * @return the shape of the variable being processed by the current job as
     * an n-dimensional array
     */
    public static int[] getVariableShape(Configuration conf) {
        if( !variableShapeSet) {
            String dimString = conf.get(VARIABLE_SHAPE_PREFIX, "");

            String[] dimStrings = dimString.split(",");
            variableShape = new int[dimStrings.length];

            for( int i=0; i<variableShape.length; i++) {
                variableShape[i] = Integer.parseInt(dimStrings[i]);
            }

            variableShapeSet = true;
        }

        return variableShape;
    }
    /**
     * Indicates if the Variable Shape has already been set for the current job
     * @return boolean true if the variable shape has been set, false otherwise
     */
    public static boolean variableShapeSet() {
      return variableShapeSet;
    }

    /**
     * Get the name of the variable currently being processed
     * @param conf Configuration object for the current job
     * @return the name of the variable being processed by the current job
     */
    public static String getVariableName(Configuration conf) {
            String varString = conf.get(VARIABLE_NAME, "");

            return varString;
    }

    /**
     * Get the name of the log file that is being logged to. Potentially an 
     * empty String
     * @param conf Configuration object for the current job
     * @return the file path for the debug file. Possibly an empty string
     */
    public static String getDebugLogFileName(Configuration conf) {
      if ( !debugLogFileSet ) {
        debugLogFile = conf.get(DEBUG_LOG_FILE, "");
        debugLogFileSet = true;
      }
        
      return debugLogFile;
    }

    /**
     * Return the configured buffer size.  
     * @param conf Configuration object for the current job
     * @return the configured buffer size for the currently executing program
     */
    public static int getBufferSize(Configuration conf) { 
      if (!bufferSizeSet ) { 
        bufferSize = Integer.parseInt(conf.get(BUFFER_SIZE, DEFAULT_BUFFER_SIZE));
        bufferSizeSet = true;
      }

      return bufferSize;
    }

    /**
     * Get the configured Multiple File mode for the current job
     * @param conf Configuration object for the current job
     * @return a MultiFileMode entry, specifing how to process multiple files
     */
    public static MultiFileMode getMultiFileMode(Configuration conf) { 
      if (!multiFileModeSet) { 
        String multiFileModeString = getMultiFileModeString(conf);
        multiFileMode = parseMultiFileMode( multiFileModeString );
        multiFileModeSet = true;
      }

      return multiFileMode;
    }

    /**
     * Get the configured sampling ratio for the current program. 
     * Specifies what percentage of the given data set to sample
     * for placement.
     * @param conf Configuration object for the current job
     * @return the sample ratio as a float between 0 and 1 
     */
    public static float getSampleRatio(Configuration conf) {
        if ( !sampleRatioSet) {
            sampleRatio = conf.getFloat(SAMPLE_RATIO, (float)0.01);
            sampleRatioSet = true;
        }

        return sampleRatio;
    }

    /**
     * Get the "low" value for filtering out data on the record dimension
     * @param conf Configuration object for the current job
     * @return the lowest valid value on the record dimension
     */
    public static int getValidLow(Configuration conf) {
        if ( !validSet ) {
            validLow = conf.getInt(Utils.LOW_FILTER, Integer.MIN_VALUE);
            validHigh = conf.getInt(Utils.HIGH_FILTER, Integer.MAX_VALUE);
            validSet = true;
        }

        return validLow;
    }

    /**
     * Get the "high" value for filtering out data on the record dimension
     * @param conf Configuration object for the current job
     * @return the highest valid value on the record dimension
     */
    public static int getValidHigh(Configuration conf) {
        if ( !validSet ) {
            validLow = conf.getInt(Utils.LOW_FILTER, Integer.MIN_VALUE);
            validHigh = conf.getInt(Utils.HIGH_FILTER, Integer.MAX_VALUE);
            validSet = true;
        }

        return validHigh;
    }

    /**
     * Get a String indiciating whether the No Scan feature is enabled
     * @param conf Configuration object for the current job
     * @return a String that is either "TRUE" or "FALSE, depending on if the
     * No Scan feature is enabled or not
     */
    public static String getNoScanString(Configuration conf) {
        return conf.get(Utils.NO_SCAN, "TRUE");  // default to true (no scan enabled)
    }

    /**
     *  Get a String indicating how many Reducers this job is configured for
     * @param conf Configuration object for the current job
     * @return a String containing the number of Reducers that this job is 
     * configured for
     */
    public static String getNumberReducersString(Configuration conf) {
        return conf.get(Utils.NUMBER_REDUCERS, "1"); // default to one reducer
    }

    /**
     * Get a String indicating whether a combiner should be employed for this job
     * @param conf Configuration object for the current job
     * @return a String with either "TRUE" or "FALSE", depending on if a combiner 
     * should be used for this job
     */
    public static String getUseCombinerString(Configuration conf) {
        return conf.get(Utils.USE_COMBINER, "TRUE");  // default to true (combiner enabled)
    }

    /**
     * Get a String indiciating whether the Query Dependent partitioning
     * feature should be used
     * @param conf Configuration object for the current job
     * @return a String with either "TRUE" or "FALSE", depending on whether
     * Query Aware partitioning should be used for this job
     */
    public static String getQueryDependantString(Configuration conf) {
        return conf.get(Utils.QUERY_DEPENDANT, "FALSE"); // default to false (not query dependent)
    }

    /**
     * Get a String indicating if the current job is a Holistic function 
     * @param conf Configuration object for the current job
     * @return a String with either "TRUE" or "FALSE" depending on whether
     * this program is applying a Holistic function
     */
    public static String getHolisticString(Configuration conf) {
        return conf.get(Utils.HOLISTIC, "FALSE"); // default to false (not holistic)
    }

    /**
     * Get a String indicating which Operator is being applied
     * @param conf Configuration object for the current job
     * @return a String with the name of the operator being applied by 
     * this program
     */
    public static String getOperatorString(Configuration conf) {
        return conf.get(Utils.OPERATOR, "");
    }

    /**
     * Get a String indicating which Mode is being used for 
     * partitioning
     * @param conf Configuration object for the current job
     * @return a String containing the Partitioning mode 
     */
    public static String getPartModeString(Configuration conf) {
        return conf.get(Utils.PART_MODE, "Record");
    }

    /**
     * Get a String indicating which Mode is being used for 
     * placement 
     * @param conf Configuration object for the current job
     * @return a String containing the Placement mode 
     */
    public static String getPlacementModeString(Configuration conf) {
        return conf.get(Utils.PLACEMENT_MODE, "Sampling");
    }

    /**
     * Get a String indicating which Mode is being used to
     * address dealing with multiple files in the input
     * @param conf Configuration object for the current job
     * @return a String containing the multiple file mode 
     */
    public static String getMultiFileModeString(Configuration conf) {
        return conf.get(Utils.MULTI_FILE_MODE, "concat");
    }
  
    /**
     * Parse a String containing the number of Reducers for this job
     * @param numberReducersString a String containing the number of 
     * reducers for the current job
     * @return the number of reducers for the current job
     */
    public static int parseNumberReducersString( String numberReducersString) {
        int retVal = 1; // reasonable default

        try { 
            retVal = Integer.parseInt(numberReducersString);
        } catch ( NumberFormatException nfe) {
            LOG.info("nfe caught in parseNumberReducersString on string " + 
                     numberReducersString + ". Using 1 as a default" );
            retVal = 1;
        }

        return retVal;
    }

    /**
     * Parse a String indicating if a combiner should be used for this job
     * @param useCombinerString indicating whether a combiner should be used
     * @return whether a combiner should be used
     */
    public static boolean parseUseCombinerString( String useCombinerString) {
      if ( 0 == useCombinerString.compareToIgnoreCase("True" ) ) {
          return true;
        } else {
            return false;
        }
    }

    /**
     * Parse a String indicating if the No Scan functionality 
     * should be used for this job
     * @param noScanString indicating whether No Scan should be used
     * @return whether No Scan functionality should be used
     */
    public static boolean parseNoScanString( String noScanString) {
      if ( 0 == noScanString.compareToIgnoreCase("True" ) ) {
          return true;
        } else {
            return false;
        }
    }

    /**
     * Parse a String indicating if Query Dependant partitioning 
     * should be used for this job
     * @param queryDependantString indicating whether Query Dependant partitioning
     * should be used
     * @return whether Query Depenedent partitioning should be used
     */
    public static boolean parseQueryDependantString( String queryDependantString) {
      if ( 0 == queryDependantString.compareToIgnoreCase("True" ) ) {
          return true;
        } else {
            return false;
        }
    }

    /**
     * Parse a String indicating if the current program is applying a holistic
     * function
     * @param holisticString indicating whether the current job is applying
     * a holistic function
     * @return whether the function being applied is holistic
     */
    public static boolean parseHolisticString( String holisticString) {
      if ( 0 == holisticString.compareToIgnoreCase("True" ) ) {
          return true;
        } else {
            return false;
        }
    }

    /**
     * Parse a String indicating how to process multiple file inputs
     * @param multiFileModeString indicates how to process input sets of 
     * multiple files
     * @return which MultiFileMode to use for this job
     */
    public static MultiFileMode parseMultiFileMode( String multiFileModeString ) {
        MultiFileMode multiFileMode = MultiFileMode.combine;

      if ( 0 == multiFileModeString.compareToIgnoreCase("Combine" ) ) {
          multiFileMode = MultiFileMode.combine;
      } else if ( 0 == multiFileModeString.compareToIgnoreCase("Concat")) {
          multiFileMode = MultiFileMode.concat;
      }

        return multiFileMode;
    }
    
    /**
     * Parse a String indicating how to process multiple file inputs
     * @param multiFileModeString indicates how to process input sets of 
     * multiple files
     * @return which MultiFileMode to use for this job
     */
    public static PlacementMode parsePlacementMode( String placementModeString ) {
        PlacementMode placementMode = PlacementMode.roundrobin;

      if ( 0 == placementModeString.compareToIgnoreCase("RoundRobin" ) ) {
          placementMode = PlacementMode.roundrobin;
      } else if ( 0 == placementModeString.compareToIgnoreCase("Sampling")) {
          placementMode = PlacementMode.sampling;
      } else if ( 0 == placementModeString.compareToIgnoreCase("Implicit")) {
          placementMode = PlacementMode.implicit;
      }

        return placementMode;
    }
   
    /**
     * Parse a String indicating which partitioning mode to use
     * @param partModeString indicates the partitioning mode to use for
     * this job
     * @return a PartMode object indicating which partitioning mode to use
     */ 
    public static PartMode parsePartMode( String partModeString ) {
        PartMode partMode = PartMode.proportional;

      if ( 0 == partModeString.compareToIgnoreCase("Proportional" ) ) {
          partMode = PartMode.proportional;
      } else if ( 0 == partModeString.compareToIgnoreCase("Record")) {
          partMode = PartMode.record;
      } else if ( 0 == partModeString.compareToIgnoreCase("Calculated")) {
          partMode = PartMode.calculated;
      } else {
          LOG.warn("Specified partition mode is not understood: " + partModeString + "\n" +
                   "Please specify one of the following: proportional, record, calculated" );

        }

        return partMode;
    }
    
    /**
     * Parse a String indicating which Operator this job is using
     * @param operatorString indicates which Operator this job is applying
     * @return an Operator object indicating which function this job is applying 
     */ 
    public static Operator parseOperator( String operatorString ) {
      Operator op;

      if ( 0 == operatorString.compareToIgnoreCase("Max")) {
          op = Operator.max;
      } else if ( 0 == operatorString.compareToIgnoreCase("SimpleMax")) {
          op = Operator.simpleMax;
      } else if ( 0 == operatorString.compareToIgnoreCase("Median")) {
          op = Operator.median;
      } else if ( 0 == operatorString.compareToIgnoreCase("SimpleMedian")) {
          op = Operator.simpleMedian;
      } else if ( 0 == operatorString.compareToIgnoreCase("NullTest")) {
          op = Operator.nulltest;
      } else if ( 0 == operatorString.compareToIgnoreCase("Average")) {
          op = Operator.average;
      } else {
          // redundant, given initialization of retVal to optUnknown but shooting for clarity
          op = Operator.optUnknown;
      }

        return op;
    }

    /**
     * Retrieve the partitioning mode for this job
     * @param conf the Configuration object for the current job
     * @return a PartMode object indicating which partitioning mode 
     * to use for this job
     */ 
    public static PartMode getPartMode( Configuration conf) {

        if( !partModeSet) {
            String partModeString = getPartModeString(conf);
            partMode = parsePartMode( partModeString );
            partModeSet = true;
        }
  
        return partMode;
    }

    /**
     * Retrieve the placement mode for this job
     * @param conf the Configuration object for the current job
     * @return a PlacmenetMode object indicating which placement mode 
     * to use for this job
     */ 
    public static PlacementMode getPlacementMode( Configuration conf) {

        if( !placementModeSet) {
            String placementModeString = getPlacementModeString(conf);
            placementMode = parsePlacementMode( placementModeString );
            placementModeSet = true;
        }
  
        return placementMode;
    }

    /**
     * Retrieve the placement mode for this job
     * @param conf the Configuration object for the current job
     * @return a PlacmenetMode object indicating which placement mode 
     * to use for this job
     */ 
    public static boolean useCombiner(Configuration conf) {
        if ( !useCombinerSet ) {
            String useCombinerString = getUseCombinerString(conf);
            useCombiner = parseUseCombinerString(useCombinerString);
            useCombinerSet = true;
        }

        return useCombiner;  // default to false (no scan not enabled)
    }

    /**
     * Retrieve the number of reducers to use for this job
     * @param conf the Configuration object for the current job
     * @return the number of reducers to use for this job
     */ 
    public static int getNumberReducers(Configuration conf) {
        if ( !numberReducersSet) {
            String numberReducersString = getNumberReducersString(conf);
            numberReducers = parseNumberReducersString(numberReducersString);
            numberReducersSet = true;
        }

        return numberReducers;  // default to false (no scan not enabled)
    }

    /**
     * Determine whether the No Scan functionality should be used
     * @param conf the Configuration object for the current job
     * @return whether to enable No Scan functionality for this job
     */ 
    public static boolean noScanEnabled(Configuration conf) {
        if ( !noScanEnabledSet ) {
            String noScanString = getNoScanString(conf);
            noScanEnabled = parseNoScanString(noScanString);
            noScanEnabledSet = true;
        }
        return noScanEnabled;  // default to false (no scan not enabled)
    }

    /**
     * Determine whether the Query Dependant functionality should be used
     * @param conf the Configuration object for the current job
     * @return whether to enable Query Dependnet functionality for this job
     */ 
    public static boolean queryDependantEnabled(Configuration conf) {
        if ( !queryDependantEnabledSet ) {
            String queryDependantString = getQueryDependantString(conf);
            queryDependantEnabled = parseQueryDependantString(queryDependantString);
            queryDependantEnabledSet = true;
        }
        return queryDependantEnabled;  // default to false (no scan not enabled)
    }

    /**
     * Determine whether the current function is holistic
     * @param conf the Configuration object for the current job
     * @return whether the current query is holistic
     */ 
    public static boolean holisticEnabled(Configuration conf) {
        if ( !holisticEnabledSet ) {
            String holisticString = getHolisticString(conf);

            holisticEnabled = parseHolisticString(holisticString);

            holisticEnabledSet = true;
        }

        return holisticEnabled;  // default to false (no scan not enabled)
    }


    /**
     * Retrieve the current Operator
     * @param conf the Configuration object for the current job
     * @return an Operator object indicating which function is being applied
     * by the current program
     */ 
    public static Operator getOperator( Configuration conf) {

        if( !operatorSet) {
            String operatorString = getOperatorString(conf);

            operator = parseOperator( operatorString);

            operatorSet = true;
        }
  
        return operator;
    }

    /**
     * Retrieve the extraction shape for the current job
     * @param conf the Configuration object for the current job
     * @return the extraction shape for this job as an n-dimensional array 
     */ 
    public static int[] getExtractionShape(Configuration conf, int size) {
        if( !extractionShapeSet) {
            String extractionString = conf.get(EXTRACTION_SHAPE, "");

            if ( extractionString == "" ) {

                extractionShape = new int[size];

                for( int i=0; i<size; i++) {
                    extractionShape[i] = 1;
                }

            } else {
                String[] extractionDims = extractionString.split(",");

                extractionShape = new int[extractionDims.length];

                for( int i=0; i < extractionShape.length; i++) {
                    extractionShape[i] = Integer.parseInt(extractionDims[i]);
                }
            }

            extractionShapeSet = true;
        }

        return extractionShape;
    }

    /**
     * Determine whether a given cell is valid
     * @param globalCoord an n-dimensional array containing the coordinate 
     * to validate 
     * @param conf the Configuration object for the current job
     * @return whether the indicated cell is valid
     */ 
    public static boolean isValid(int[]globalCoord, Configuration conf){

        if( !validSet){
            getValidLow(conf); // this will set high and low as a byproduct of requesting the value
        }

        if(globalCoord[0] >= validLow && globalCoord[0] < validHigh ){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Helper function that turns an array into a comma delimited 
     * String
     * @param array the array to convert into a String
     * @return a String containing the contents of the array, in order,
     * comma delimited
     */
    public static String arrayToString( int[] array ) {
        String tempStr = "";

        // Sanity check and return an empty string if there's no array to iterate over
        if ( null == array )  {
          return tempStr;
        }

        for ( int i=0; i < array.length; i++) {
            if( i > 0)
                tempStr += ",";

            tempStr += array[i];
        }

        return tempStr;
    }

    /**
     * Helper function that turns an array into a comma delimited 
     * String
     * @param array the array to convert into a String
     * @return a String containing the contents of the array, in order,
     * comma delimited
     */
    public static String arrayToString( long[] array ) {
        String tempStr = "";

        for ( int i=0; i < array.length; i++) {
            if( i > 0)
                tempStr += ",";

            tempStr += array[i];
        }

        return tempStr;
    }


    /**
     * Determine if a set of cordinates are at, or beyond, the last valid 
     * data coordinate
     * @param varShape the shape of the variable being processed
     * @param current the n-dimensional coordinate being validated
     * @return whether the array being passed in is at or past the 
     * end of the variable
     */
    public static boolean endOfVariable(int[] varShape, int[] current) {
        boolean retVal = true;

        if ( current[0] >= varShape[0] ) { 
            return retVal;
        } 

        for ( int i=0; i<current.length; i++) {
            if( current[i] < varShape[i] -1 ) { 
                retVal = false;
                return retVal;
           } 
        }

        return retVal;
    }

    /**
     * Determine if the current coordinate is a "full" record
     * @param corner the coordinate to check
     * @return whether the coordinate passed in is the start of a 
     * new record 
     */
    public static boolean atFullRecord(int[] corner ) {
        boolean retVal = true;

        for ( int i = 1; i < corner.length; i++ ) {
            if ( corner[i] != 0 ) { 
                retVal = false; 
            }   
        }   

        return retVal;
    } 

    /**
     * Increment an array in the context of an n-dimensional variable
     * @param varShape the variable being processed by the current program
     * @param current the coordinate to increment
     * @return the incremented n-dimensional array
     */
    public static int[] incrementArray( int[] varShape, int[] current ) {
        int curDim = current.length - 1;
        current[curDim]++;

        while ( current[curDim] >= varShape[curDim] && curDim > 0 ) {
            current[curDim] = 0;
            current[curDim - 1]++;
            curDim--;
        }

        return current;
    }
        

    /**
     * Caculates the total number of cells present in an n-dimensional array
     * @param array the n-dimensional array for which to calculate the total size
     * @return the count of cells present in the array
     */
    public static int calcTotalSize( int[] array ) {
        int retVal = 1;

        for( int i=0; i<array.length; i++) {
            retVal *= array[i];
        }

        return retVal;
    }

    /**
     * Caculates the size of an n-dimensional array in bytes
     * @param array the n-dimensional array for which to calculate the total size
     * @param dataTypeSize the size of the data type, in bytes, 
     * stored in the array
     * @return the size of the n-dimensional array, in bytes
     */
    public static long calcArrayTotalSize( int[] array, int dataTypeSize ) { 
        long retVal = 1;

        for( int i=0; i<array.length; i++) {
            retVal *= array[i];
        }

        retVal *= dataTypeSize;

        return retVal;
    }
  
    /**
     * Compute the number of cells needed to increment each dimension
     * of an n-dimensional shape 
     * @param shape the shape of the variable being processed
     * @return an array of longs, indicating the number of cells needed, on 
     * each dimension, to increment a coordinate on that dimension
     */
    public static long[] computeStrides(int[] shape) throws IOException {
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
     *  Expand a flattened n-dimensional array, using the variable it references
     * to calculate the said array
     * @param variableShape the shape of the variable the flattened coordinate 
     * corresponds to
     * @param element the flattened coordinate to expand
     * @return an n-dimensional coordinate
     */
    public static int[] inflate( int[] variableShape, long element ) throws IOException {
        int[] retArray = new int[variableShape.length];
       
        long[] strides = computeStrides( variableShape );

        for ( int i = 0; i < variableShape.length; i++) {
            retArray[i] = (int)(element / strides[i]);
            element = element - (retArray[i] * strides[i] );    
        }

        return retArray;
    } 

    /**
     * Flattens an n-dimensional coordinate into a single long value
     * @param variableShape the shape of the variable that the coordinate
     * references
     * @param currentElement the coordinate to flatten
     * @return a long value that is the equivalent of the currentElement arguement
     */
    public static long flatten( int[] variableShape, int[] currentElement ) throws IOException {
        return calcLinearElementNumber( variableShape, currentElement );
    }

    /**
     * Flattens an n-dimensional coordinate into a single long value
     * @param variableShape the shape of the variable that the coordinate
     * references
     * @param currentElement the coordinate to flatten
     * @return a long value that is the equivalent of the currentElement arguement
     */
    public static long calcLinearElementNumber( int[] variableShape, int[] currentElement ) 
                                                throws IOException {
        
        long[] strides = computeStrides( variableShape);

        long retVal = 0;

        for( int i=0; i<currentElement.length; i++ ) {
            retVal += ( strides[i] * currentElement[i] );
        }

        return retVal;
    }

    /**
     * Determines whether an array is sorted. Probably superfluous
     * @param array the array to determine whether it's sorted
     * @return whether the array is sorted
     */
    public static boolean isSorted( int[] array ){
      if ( array.length <= 1) { 
        return true;
      }

      for ( int i=0; i<array.length - 1; i++) {
        if ( array[i] > array[ i+1]) {
          return false;
        }
      }

      return true;
    }

    /**
     * Determines whether an array is sorted. Probably superfluous
     * @param array the array to determine whether it's sorted
     * @return whether the array is sorted
     */
    public static boolean isSorted( long[] array ){
      if ( array.length <= 1) { 
        return true;
      }

      for ( int i=0; i<array.length - 1; i++) {
        if ( array[i] > array[ i+1]) {
          return false;
        }
      }

      return true;
    }

    /**
     * This method maps an offset to a BlockLocation
     * @param blocks A lits of BlockLocations that represent the file 
     * containing the variable in question
     * @param offset An offset in the byte-stream
     * @return the BlockLocation which contains the offset passed into 
     * this method
     */
    public static BlockLocation offsetToBlock(BlockLocation[] blocks, long offset) {
        for (BlockLocation block : blocks) {
        long start = block.getOffset();
        long end = start + block.getLength();
        if (start <= offset && offset < end)
            return block;
        }
        return null;
    }

    /**
     * Maps a local coordinate into the global space
     * @param currentCounter the current ID to map into the global space
     * @param corner the anchoring corner of the current data set
     * @param globalCoordinate return value for this function
     */
    public static int[] mapToGlobal( int[] currentCounter, int[] corner,
                               int[] globalCoordinate) {
      for ( int i=0; i < currentCounter.length; i++) {
        globalCoordinate[i] = currentCounter[i] + corner[i];
      }
      return globalCoordinate;
    }

    public static void adjustGIDForLogicalOffset( GroupID gid, 
                                                  int[] logicalStartOffset, 
                                                  int[] extractionShape ) { 
      int[] groupID = gid.getGroupID();
      for( int i=0; i<logicalStartOffset.length; i++) { 
        groupID[i] += (logicalStartOffset[i] / extractionShape[i]);
      }
      gid.setGroupID(groupID);
    }

    /**
     * Map a global coordinate to a local coordinate by using the 
     * variable shape and extraction shape.
     * @param globalCoord the global coordinate to map into the local
     * space
     * @param groupIDArray memory that is allocated to hold the temporary
     * result
     * @param outGroupID the GroupID object that will be returned with
     * the local coordinate result
     * @param extractionShape extraction shape used to map from global
     * to local
     * @return a GroupID object containing the local coordinate result
     */
    public static GroupID mapToLocal( int[] globalCoord, int[] groupIDArray,
                                      GroupID outGroupID,
                                      int[] extractionShape ) {
      //short circuit out in case extraction shape is not set
      if ( extractionShape.length == 0 ) {
        outGroupID.setGroupID(groupIDArray);
        return outGroupID;
      }

      for ( int i=0; i < groupIDArray.length; i++ ) {
        groupIDArray[i] = globalCoord[i] / extractionShape[i];
      }

      outGroupID.setGroupID(groupIDArray);
      return outGroupID;
    }

    /**
     * Helper function used for debugging when needed
     * @param corner a corner coordiate
     * @param currentCounter another n-dimensional coordinate
     * @param globalCoordinate the globalcoordinate for the current coordinate
     * @param myGroupID the group ID for this coordinate
     * @param val1 value to print out
     * @param val2 value to print out
     * @return a String-ified version of all the arguements passed in 
     */
	  public static String giantFormattedPrint(int[] corner, int[] currentCounter,
	                                     int[] globalCoordinate, int[] myGroupID, 
	                                     int val1, int val2, String retString) {
	    // formatted string from hell
	    retString = String.format(
	            "gl co:%03d,%03d,%03d,%03d " +
	            "co: %03d,%03d,%03d,%03d " +
	            "ctr: %03d,%03d,%03d,%03d " +
	            "grp id: %03d,%03d,%03d,%03d " +
	            "v1: %010d " +
	            "v2: %010d" + 
	            "\n",
	            globalCoordinate[0], globalCoordinate[1], 
	            globalCoordinate[2], globalCoordinate[3],
	            corner[0], corner[1], corner[2], corner[3],
	            currentCounter[0], currentCounter[1], 
	            currentCounter[2], currentCounter[3],
	            myGroupID[0], myGroupID[1], myGroupID[2], myGroupID[3],
	            val1,
	            val2);
	
	    return retString;
    }
}
