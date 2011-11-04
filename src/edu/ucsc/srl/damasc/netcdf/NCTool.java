package edu.ucsc.srl.damasc.netcdf;

import org.apache.hadoop.util.ProgramDriver;

import edu.ucsc.srl.damasc.netcdf.tools.Identity;
import edu.ucsc.srl.damasc.netcdf.tools.Average;
import edu.ucsc.srl.damasc.netcdf.tools.Max;
import edu.ucsc.srl.damasc.netcdf.tools.Median;

/**
 * Helper class that registers the various functions that
 * SciHadoop supports
 */
public class NCTool {
  public static void main(String[] args) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("average", Average.class, "NetCDF average job");
      pgd.addClass("identity", Identity.class, "NetCDF identity job");
      pgd.addClass("max", Max.class, "NetCDF max job");
      pgd.addClass("median", Median.class, "NetCDF median job");

      //exitCode = pgd.driver(args);
      pgd.driver(args);
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}
