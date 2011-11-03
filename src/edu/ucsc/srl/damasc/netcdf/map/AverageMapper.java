package edu.ucsc.srl.damasc.netcdf.map;

import java.io.IOException;

import java.lang.Integer;
import java.lang.Long;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.IndexIterator;
import edu.ucsc.srl.damasc.netcdf.io.AverageResult;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.GroupIDGen;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * Mapper for the Average operator
 */
public class AverageMapper extends Mapper<ArraySpec, Array, LongWritable, AverageResult> {

  private static final Log LOG = LogFactory.getLog(AverageMapper.class);
  private static boolean _benchmarkArraySpec = true;

  public static enum InvalidCell { INVALID_CELL_COUNT } ;

  /**
   * Reduces values for a given key
   * @param key the Key for the given value being passed in
   * @param value an Array to process that corresponds to the given key 
   * @param context the Context object for the currently executing job
   */
  public void map(ArraySpec key, Array value, Context context)
                  throws IOException, InterruptedException {
    try { 
  
      long timerA = System.currentTimeMillis();
  
      ArrayInt ncArray = (ArrayInt)value;
  
      int[] allOnes = new int[key.getShape().length];
      for( int i=0; i<allOnes.length; i++) {
        allOnes[i] = 1;
      }
  
      int[] extractionShape = 
          Utils.getExtractionShape(context.getConfiguration(), 
                                   key.getShape().length);
      int[] variableShape =
           Utils.getVariableShape(context.getConfiguration());
  
      HashMap<GroupID, Array> groupSubArrayMap = new HashMap<GroupID, Array>();
  
      GroupIDGen myGIDG = new GroupIDGen();
      GroupIDGen.pullOutSubArrays( myGIDG, ncArray, key, extractionShape, 
                                   allOnes, groupSubArrayMap);
      LOG.info("pullOutSubArrays returned " + groupSubArrayMap.size() +  " elements");
  
      ArrayInt localArray;
      GroupID localGID = new GroupID();
      localGID.setName(key.getVarName());
      int tempInt = 0;

      int[] zeroArray = new int[extractionShape.length];
      for( int i = 0; i < zeroArray.length; i++) { 
        zeroArray[i] = 0;
      }

      int[] helpArray = new int[extractionShape.length];
      for( int i = 0; i < helpArray.length; i++) { 
        helpArray[i] = 0;
      }

      GroupID zeroGID = new GroupID(zeroArray, "windspeed1");
  
      int invalidMapperCounter = 0;
  
      LongWritable myLongW = new LongWritable();
      AverageResult avgResult = new AverageResult();
 
      // debugging bit here
      Iterator<Map.Entry<GroupID, Array>> gidItr2 = 
            groupSubArrayMap.entrySet().iterator();
 
      System.out.println("ArraySpec corner: " + Utils.arrayToString(key.getCorner()) + 
                         " shape: " + Utils.arrayToString(key.getShape())); 
      while (gidItr2.hasNext() ) { 
        System.out.println("gid: " + Utils.arrayToString(gidItr2.next().getKey().getGroupID()));
      }

      Iterator<Map.Entry<GroupID, Array>> gidItr = 
            groupSubArrayMap.entrySet().iterator();
  
      while (gidItr.hasNext() ) { 
        Map.Entry<GroupID, Array> pairs = gidItr.next();
        localGID = pairs.getKey();
        localArray = (ArrayInt)pairs.getValue();
        avgResult.clear(); // reset this variable
  
        // TODO sort out how to do filtering with this new GroupID based setup
        // -jbuck
  
        IndexIterator valItr = localArray.getIndexIterator();
  
        while( valItr.hasNext() ) {
          tempInt = valItr.getIntNext();
          avgResult.addValue(tempInt);
        }
  
        Utils.adjustGIDForLogicalOffset(localGID, key.getLogicalStartOffset(), extractionShape );

        myLongW.set( localGID.flatten(variableShape) );
  
        context.write(myLongW, avgResult);

        LOG.info("ArraySpec corner: " + Utils.arrayToString(key.getCorner()) + 
                           " shape: " + Utils.arrayToString(key.getShape()) + 
                           " logical start: " + Utils.arrayToString(key.getLogicalStartOffset()) + 
                           " extraction shape: " + Utils.arrayToString(extractionShape) + 
                           " localGID: " + Utils.arrayToString(localGID.getGroupID())
                          );
  
      }
  
      timerA = System.currentTimeMillis() - timerA;
      LOG.info("for corner " + Utils.arrayToString(key.getCorner()) + 
               " map loop time: " + 
               timerA + " ms with " );
     } catch ( Exception e ) { 
       System.out.println( " Exception caught in Average.map(). " + e.toString() );
     }
  }
}
