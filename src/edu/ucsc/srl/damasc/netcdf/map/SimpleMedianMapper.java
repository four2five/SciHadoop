package edu.ucsc.srl.damasc.netcdf.map;

import java.io.IOException;

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
import edu.ucsc.srl.damasc.netcdf.io.HolisticResult;
import edu.ucsc.srl.damasc.netcdf.io.Result;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * Mapper for the Median function that uses simple data structures
 * for the key
 */
public class SimpleMedianMapper 
       extends Mapper<ArraySpec, Array, LongWritable, HolisticResult> {

  private static final Log LOG = LogFactory.getLog(SimpleMedianMapper.class);
  private static boolean _benchmarkArraySpec = true;

  static enum MapOutputsCreated{ MAP }

  public static enum InvalidCell { INVALID_CELL_COUNT } ;


 /**
 * Reduces values for a given key
 * @param key ArraySpec representing the given Array being passed in
 * @param value an Array to process that corresponds to the given key 
 * @param context the Context object for the currently executing job
 */
  public void map(ArraySpec key, Array value, Context context)
                  throws IOException, InterruptedException {

    ArrayInt intArray = (ArrayInt)value;

    int[] globalCoord = new int[key.getShape().length];
    int[] groupIDArray = new int[key.getShape().length];

    int[] allOnes = new int[key.getShape().length];
    for( int i=0; i<allOnes.length; i++) {
      allOnes[i] = 1;
    }

    IndexIterator iter = intArray.getIndexIterator();

    GroupID myGroupID = new GroupID();
    myGroupID.setName("");

    // the two output classes
    LongWritable myLong = new LongWritable();
    HolisticResult myHolisticResult = new HolisticResult();

    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 key.getShape().length);
    int[] variableShape = Utils.getVariableShape(context.getConfiguration());

    int invalidMapperCounter = 0;
    long totalCellsSeen = 0;
    long timerA = System.currentTimeMillis();


    while( iter.hasNext() ) {
      // get a legit value first
      myHolisticResult.setHolisticResult(iter.getIntNext());

      // orient this cell's location in the global space
      globalCoord = Utils.mapToGlobal(iter.getCurrentCounter(), 
                                      key.getCorner(), globalCoord );

      // track cells that are invalid. This is typically due 
      // to filtering of cells by the query
      if ( !Utils.noScanEnabled(context.getConfiguration()) ) {
        if ( !Utils.isValid(globalCoord, context.getConfiguration()) ) {
          context.getCounter(InvalidCell.INVALID_CELL_COUNT).increment(1);
          invalidMapperCounter++;
          continue;
        }
      }

      // figure out the "bucket" for this (determined by the 
      // extraction shape and position of this cell

      myGroupID = Utils.mapToLocal(globalCoord, groupIDArray, myGroupID, 
                                   extractionShape); 

      myLong.set( myGroupID.flatten(variableShape) );
      context.write(myLong, myHolisticResult);
      context.getCounter(MapOutputsCreated.MAP).increment(1);
      totalCellsSeen++;
    }

    timerA = System.currentTimeMillis() - timerA;
    LOG.info("for corner " + Utils.arrayToString(key.getCorner()) + 
             " map loop time (ms) : " + 
             timerA + " with " + invalidMapperCounter + 
             " invalid ArraySpecs " + 
             " and " + totalCellsSeen + " total cells seen");
  }
}
