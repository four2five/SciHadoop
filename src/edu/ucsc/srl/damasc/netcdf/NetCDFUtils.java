package edu.ucsc.srl.damasc.netcdf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;

import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;

import ucar.ma2.Array;

/**
 * Utility methods that are specific to NetCDF files / data
 */
public class NetCDFUtils { 
  
  /**
   * Logs the IDs generated for a given Key / Value pair
   * (ArraySpec / Array objects)
   * @param debugFileName file to log this data to
   * @param ncArray a NetCDF Array object that is the data 
   * for the "value" part of the 
   * key / value pair
   * @param key an ArraySpec object that is the key 
   * for the "key" value pair. An ArraySpec
   * @param extractionShape the extraction shape specified
   * for this query
   * @param groupSubArrayMap mapping from GroupIDs to data
   * @param LOG the log object to write to in case of an exception
   */
  public static void logGIDs( String debugFileName, Array ncArray, 
                              ArraySpec key, int[] extractionShape, 
                              HashMap<GroupID, Array> groupSubArrayMap,
                              Log LOG) {
    try { 
      File outputFile = new File( debugFileName );
      BufferedWriter writer = 
        new BufferedWriter( new FileWriter(outputFile, true));

      Set<Map.Entry<GroupID, Array>> set = groupSubArrayMap.entrySet();

      writer.write("InputSplit: " + key);
      writer.newLine();
      for( Map.Entry<GroupID, Array> me : set ) {
        writer.write("\tgid: " + me.getKey().toString(extractionShape) + 
        "\tspec: " + Utils.arrayToString(me.getValue().getShape()) );
        writer.newLine();
      }
      writer.close();

    } catch ( IOException ioe ) {
      LOG.error("Caught an ioe in MedianMapper.logGIDS()\n" + ioe.toString());
    }
  }
}
