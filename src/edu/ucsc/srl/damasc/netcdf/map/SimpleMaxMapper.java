package edu.ucsc.srl.damasc.netcdf.map;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.Result;
import edu.ucsc.srl.damasc.netcdf.Utils;

import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;

/**
 * Mapper for the Max function that uses simple data structures as 
 * keys
 */
public class SimpleMaxMapper 
       extends Mapper<ArraySpec, Array, LongWritable, IntWritable> {

  private static final Log LOG = LogFactory.getLog(SimpleMaxMapper.class);
  private static boolean _benchmarkArraySpec = true;

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

    //Index myIndex = new Index(key.getShape(), allOnes); 
    IndexIterator iter = intArray.getIndexIterator();

    GroupID myGroupID = new GroupID();
    myGroupID.setName(key.getVarName());

    //Result myResult = new Result();
    IntWritable myInt = new IntWritable();
    LongWritable myLong = new LongWritable();

    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 key.getShape().length);
    int[] variableShape = 
        Utils.getVariableShape(context.getConfiguration());

    //OptMode optMode = Utils.getOptMode(context.getConfiguration() );
    //PartMode partMode = Utils.getPartMode(context.getConfiguration() );

    int invalidMapperCounter = 0;
    int val = 0;
    long timerA = System.currentTimeMillis();


    while( iter.hasNext() ) {

      val = iter.getIntNext();

      // orient this cell's location in the global space
      globalCoord = Utils.mapToGlobal(iter.getCurrentCounter(), 
                                key.getCorner(), globalCoord );

      // track cells that are invalid. This is typically due 
      // to filtering of cells by the query
      // it's only applied at the mapper for optB. 
      // optC applies it at split generation

      if ( !Utils.noScanEnabled(context.getConfiguration()) ) 
      { 
        if ( !Utils.isValid(globalCoord, context.getConfiguration()) ) {
          context.getCounter(InvalidCell.INVALID_CELL_COUNT).increment(1);
          continue;
        }
      }

      // figure out the "bucket" for this (determined by the 
      // extraction shape and position of this cell
      myGroupID = Utils.mapToLocal(globalCoord, groupIDArray, 
                                   myGroupID, extractionShape); 

      myInt.set( val );
      myLong.set( myGroupID.flatten(variableShape) );

      context.write(myLong, myInt);
    }

    timerA = System.currentTimeMillis() - timerA;
    LOG.info("for corner " + Utils.arrayToString(key.getCorner()) + 
             " map loop time (ms) : " + 
             timerA + " with " + invalidMapperCounter + 
             " invalid ArraySpecs" );

  }
}
