package edu.ucsc.srl.damasc.netcdf.map;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.GroupIDGen;
import edu.ucsc.srl.damasc.netcdf.io.HolisticResult;
import edu.ucsc.srl.damasc.netcdf.io.Result;
import edu.ucsc.srl.damasc.netcdf.Utils;
import edu.ucsc.srl.damasc.netcdf.NetCDFUtils;

/**
 * Mapper that prepares data for the Median operation
 */
public class MedianMapper extends 
      Mapper<ArraySpec, Array, GroupID, HolisticResult> {

  private static final Log LOG = LogFactory.getLog(MedianMapper.class);
  private static boolean _benchmarkArraySpec = true;

  public static enum MedianMapStatus { FULL, NOTFULL }

 /**
 * Reduces values for a given key
 * @param key ArraySpec representing the given Array being passed in
 * @param value an Array to process that corresponds to the given key 
 * @param context the Context object for the currently executing job
 */
  public void map(ArraySpec key, Array value, Context context)
                  throws IOException, InterruptedException {

    long timerA = System.currentTimeMillis();

    ArrayInt ncArray = (ArrayInt)value;

    int[] allOnes = new int[key.getShape().length];
    for( int i=0; i<allOnes.length; i++) {
      allOnes[i] = 1;
    }

    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 key.getShape().length);

    HashMap<GroupID, Array> groupSubArrayMap = new HashMap<GroupID, Array>();

    GroupIDGen myGIDG = new GroupIDGen();
    GroupIDGen.pullOutSubArrays( myGIDG, ncArray, key, extractionShape,
                                 allOnes, groupSubArrayMap);

    String debugFileName = Utils.getDebugLogFileName(context.getConfiguration());
    if ( "" != debugFileName ) {
      LOG.info("Trying to log to " + debugFileName);
      NetCDFUtils.logGIDs( debugFileName, ncArray, key, 
                           extractionShape, groupSubArrayMap, LOG );
    }

    ArrayInt localArray;
    GroupID localGID;

    int currentMax = Integer.MIN_VALUE;
    int tempValue = Integer.MIN_VALUE;
    int invalidMapperCounter = 0;
    long totalCellsSeen = 0;
    long counter = 0;
    int finalValue = Integer.MIN_VALUE;

    // the two output classes
    HolisticResult outputHolisticResult = new HolisticResult();

    Iterator<Map.Entry<GroupID, Array>> gidItr = 
                groupSubArrayMap.entrySet().iterator();


    while( gidItr.hasNext() ) {
        counter = 0;
        Map.Entry<GroupID, Array> pair = gidItr.next();
        localGID = pair.getKey();
        localArray = (ArrayInt)pair.getValue();

        outputHolisticResult.clear();

        // create a holistic result big enough to hold all the data that it 
        // may see (note, we know how many it will see but 
        // this seems like a safe move)
        outputHolisticResult.setNeededValueCount( 
                Utils.calcTotalSize(extractionShape) );

        IndexIterator valItr = localArray.getIndexIterator();

        LOG.info("exShapeSize: " + Utils.calcTotalSize(extractionShape) + 
                 " holisticResultSize: " + localArray.getSize() );

        while( valItr.hasNext() ) {
            //LOG.info("gid: " + Utils.arrayToString( localGID.getGroupID()) + 
            //         " adding element " + counter + " at: " + 
            //  outputHolisticResult.getCurrentValueCount());
            // get a legit value first
            outputHolisticResult.setValue(valItr.getIntNext());
            counter++;
        }

        // if we have a full HolisticResult, 
        // we should consolidate it into a final answer
        // prior to writing it out
        if( outputHolisticResult.isFull() ){
          outputHolisticResult.sort();

          if ( !Utils.isSorted( outputHolisticResult.getValues())) {
            LOG.error("Holistic result for GID: " + key + " has unsorted results");
          }

          finalValue = outputHolisticResult.getValues()[outputHolisticResult.getValues().length/2 ];
          outputHolisticResult.setFinal(finalValue);
          context.getCounter(MedianMapStatus.FULL).increment(1);
          LOG.info("GID " + localGID + " is full in the mapper");
        } else {
          context.getCounter(MedianMapStatus.NOTFULL).increment(1);
          LOG.info("GID " + localGID + " is NOT full in the mapper, has " + 
                   outputHolisticResult.getCurrentValueCount() + " elements" );
        }

        context.write(localGID, outputHolisticResult);

        totalCellsSeen += localArray.getSize();
    }

    timerA = System.currentTimeMillis() - timerA;
    LOG.info("for corner " + Utils.arrayToString(key.getCorner()) + 
             " map loop time: " + 
             timerA + " ms with " + 
             + totalCellsSeen + " total cells seen");
  }
}
