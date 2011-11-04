package edu.ucsc.srl.damasc.netcdf.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.GroupID;
import edu.ucsc.srl.damasc.netcdf.io.Result;
import edu.ucsc.srl.damasc.netcdf.Utils;

/**
 * This class generates GroupIDs by iterating the extraction shape
 * over the specified logical space of input
 */
public class GroupIDGen { 

    public static boolean debug = false;
    private static final Log LOG = LogFactory.getLog(GroupIDGen.class);


    public GroupIDGen() { 
    }

    /**
     * Translate an n-dimensional coordinate from its local space
     * to the global space.
     * @param currentCounter the current global coordinate(s)
     * @param corner the current local coordinate
     * @param globalCoordinate object to hold the return value
     * @return the global coordinate for this local coordinate
     */
    private int[] mapToGlobal( int[] currentCounter, int[] corner, 
                               int[] globalCoordinate) {
        for ( int i=0; i < currentCounter.length; i++) {
            globalCoordinate[i] = currentCounter[i] + corner[i];
        }

        return globalCoordinate;
    }

    /**
     * Map from a global coordinate to a local coordinate. Local means relative to a given instance
     * of the extraction shape
     * @param globalCoord an absolute global coordinate
     * @param groupIDArray an array representing the local coordinate
     * @param outGroupID the return object, a GroupID object
     * @param extractionShape the extraction shape being used by the currently running MR program
     * @param a GroupID object representing the local coordinates resulting from dividing the 
     * global ID by the extraction shape
     */
    private GroupID normalizeGroupID( int[] globalCoord, int[] groupIDArray, 
                                      GroupID outGroupID, int[] extractionShape ) {

        // short circuit out in case extraction shape is not set
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
     * This is called recursively to iterate the extraction shape over the logical space.
     * The result is a set of GroupID objects that cover the logical input space
     * note, in a 4-d array, the fastest changing dimension is the highest (len - 1)
     * NOTE: this will produce GroupIDs that bound the corner & shape (and exceed them)
     * @param groupIDs an ArrayList of GroupID objects, this stores the produced GroupIDs
     * @param corner logical corner for the space that GroupIDs are being generated for
     * @param shape shape of the logical space that GroupIDs are being generated for
     * @param exShape extraction shape used to generate the individual groups (GroupIDs)
     * @param curGroup current GroupID being calculated
     * @param curDim current dimension that is being incremented as GroupIDs are being
     * generated
     * @param levelFull an array that tracks which levels of the logical space have been
     * exhausted in terms of having GroupIDs covering them completely
     */
    public void recursiveGetBounding(ArrayList<GroupID> groupIDs, int[] corner,  int[] shape,
                                     int[] exShape, GroupID curGroup, 
                                     int curDim, boolean[] levelFull) {
      try {
        LOG.info("recur, dim: " + curDim + " curGroup: " + curGroup );

        while ( levelFull[0] == false ) { // while not done
          // have we hit the high dimension yet ?
          if ( curDim < corner.length - 1 ) {
            if (levelFull[curDim + 1] == false ) { 
              // keep going down as far as you can
              recursiveGetBounding(groupIDs, corner, shape, exShape, curGroup,
                                    (curDim + 1), levelFull);
              //return;
            } else { // lower level(s) are full, try incrementing this level
              if ( (curGroup.getGroupID()[curDim] + 1) * exShape[curDim] <
                    corner[curDim] + shape[curDim] ) { 
                // increment this level and then blank the lower levels (both full and curGroup)
                curGroup.setDimension(curDim, (curGroup.getGroupID()[curDim]) + 1);

                // blank level full curGroup values for higher dimensions
                for ( int i=curDim + 1; i < corner.length; i++) {
                  levelFull[i] = false;
                  curGroup.setDimension(i, 0);
                }

                //return; // this will drop down a level next time

              } else { // this means this level is full
                levelFull[curDim] = true;
                return;
              }
            }
          } else { // hit top (fasted varying) (curDim == corner.length)
            while ( curGroup.getGroupID()[curDim] * exShape[curDim] < 
                    corner[curDim] + shape[curDim] ) {
              // NOTE: we only add groupIDs at the zero level
              /*
              System.out.println("curDim: " + curDim);
              System.out.println( (curGroup.getGroupID()[curDim] * exShape[curDim])  +" < " + 
                                  (corner[curDim] + shape[curDim]) );
              */
              LOG.info("\taddingGroup(): " + curGroup);
              groupIDs.add( new GroupID(curGroup.getGroupID(), ""));
              curGroup.setDimension(curDim, (curGroup.getGroupID()[curDim]) + 1);
              /*
              System.out.println( (curGroup.getGroupID()[curDim] * exShape[curDim])  +" < " + 
                                  (corner[curDim] + shape[curDim]) );
              */
            }
            LOG.info("Setting level " + curDim + " to full");
            levelFull[curDim] = true;
            return;
          }

        } // levelFull[levelFull.length - 1] == false

      } catch ( Exception e ) { 
        System.out.println("caught e in IdentityMapper.recursiveGetBounding. \n" + e.toString() );
        System.out.println("curDim: " + curDim + " groupID: " + curGroup.toString());
      }
    }

    /**
     * Helper function for splitting out the components of ArraySpec
     * @param key ArraySpec object representing the logical space to generate GroupIDs for
     * @param extractionShape the shape that dictates how the logical space is decomposed
     * @return an array containing the generated GroupIDs
     */
    public GroupID[] getBoundingGroupIDs( ArraySpec key, int[] extractionShape) {
        return getBoundingGroupIDs(key.getCorner(), key.getShape(), extractionShape );
    }

    /**
     * Taks a corner, shape and the extraction shape to tile over it and returns an array
     * of GroupID objects that completely covers the logical space defined by 
     * corner and shape
     * @param corner the corner of the logical space to create GroupIDs for
     * @param shape the shape of the logical space to generate GroupIDs for
     * @param extractionShape the shape to be tiled over the logical space when generating 
     * GroupID objects
     * @return an array of GroupID objects the completely covers the logical space defined by
     * the fucntion inputs
     */
    public GroupID[] getBoundingGroupIDs( int[] corner, int[]  shape, 
                                          int[] extractionShape) {
        // we'll need to do this recursively as this will need to go down n-dimensions deep

        ArrayList<GroupID> groupIDs = new ArrayList<GroupID>();
        int curDimension = extractionShape.length;
        GroupID[] returnArray = new GroupID[0];

        // seed the recursive method with the groupID of the first point in the corner
        int[] groupIDArray = new int[extractionShape.length];

        // track which levels are full
        boolean[] levelFull = new boolean[extractionShape.length];

        for( int i=0; i<groupIDArray.length; i++){
            groupIDArray[i] = 0;
            levelFull[i] = false;
        }

        try { 
            GroupID myGroupID = new GroupID(groupIDArray, "");
            // groupIDArray is just a placeholder variable for use in the function
            myGroupID = normalizeGroupID(corner, groupIDArray, myGroupID, extractionShape); 

            recursiveGetBounding(groupIDs, corner, shape, extractionShape, 
                                 myGroupID, 0, levelFull); 

            // return the array
            returnArray = new GroupID[groupIDs.size()];
            returnArray = groupIDs.toArray(returnArray);
        } catch ( Exception e ) {
            System.out.println("Caught exception in IdentityMapper.getBoundingGroupIDs() \n" + 
                               e.toString() );
        }

        return returnArray;
    }

    /**
     * Helper function that populates an array, of the size specified 
     * by variableShape. Typically used for generating data for testing.
     * @param arrayShape shape of the data to be generated
     * @return an ArrayInt object full of data
     */
    private static ArrayInt populateArray( int[] arrayShape ) {
        ArrayInt returnArray = new ArrayInt( arrayShape );

        IndexIterator itr = returnArray.getIndexIterator();

        int counter = 0;

        while( itr.hasNext() ) {
            itr.next();
            itr.setIntCurrent(counter);
            counter++;
        }

        return returnArray;
    }

    /**
     * This method creates an Array spec that is the intersection of 
     * the current GroupID (normalized into global space) and the actual
     * array being tiled over. The returned value will be smaller than or equal
     * to the extraction shape for any given dimension.
     * @param spec The corner / shape of the data we want
     * @param gid The GroupID for the current tiling of the extraction shape
     * @param extractionShape the extraction shape
     * @param ones an array of all ones (needed by calls in this function)
     * @return returns an ArraySpec object representing the data 
    */
    private static ArraySpec getGIDArraySpec( ArraySpec spec, 
                                              GroupID gid, int[] extractionShape, 
                                              int[] ones) {

        int[] normalizedGID = new int[extractionShape.length];
        int[] groupID = gid.getGroupID();

        // project the groupID into the global space, via extractionShape, 
        // and then add the startOffset
        for( int i=0; i < normalizedGID.length; i++) {
            normalizedGID[i] = (groupID[i] * extractionShape[i]); 
        }
        //System.out.println("normalized: " + Utils.arrayToString(normalizedGID));
        
        // we're going to adjust normaliedGID to be the corner of the subArray
        // need a new int[] for the shape of the subArray
        
        int[] newShape = new int[normalizedGID.length];

        // roll through the various dimensions, creating the correct corner / shape
        // pair for this GroupID (gid)
        for( int i=0; i < normalizedGID.length; i++) {
            // this gid starts prior to the data for this dimension
            newShape[i] = extractionShape[i];
            //newShape[i] =e 1;

            // in this dimension, if spec.getCorner is > normalizedGID,
            // then we need to shrink the shape accordingly.
            // Also, move the normalizedGID to match
            if ( normalizedGID[i] < spec.getCorner()[i]) { 
                newShape[i] = extractionShape[i] - (spec.getCorner()[i] - normalizedGID[i]);
                normalizedGID[i] = spec.getCorner()[i];
            // now, if the shape extends past the spec, shorten it again
            } else if ((normalizedGID[i] + extractionShape[i]) > 
                (spec.getCorner()[i] + spec.getShape()[i]) ){
              newShape[i] = newShape[i] - 
                            ((normalizedGID[i] + extractionShape[i]) - 
                            (spec.getCorner()[i] + spec.getShape()[i]));
            } 
        }

        // now we need to make sure this doesn't exceed the shape of the array
        // we're working off of
        for( int i=0; i < normalizedGID.length; i++) {
          if( newShape[i] > spec.getShape()[i] )  {
            newShape[i] = spec.getShape()[i];
          }
        }


        //System.out.println("\tcorner: " + Utils.arrayToString(normalizedGID) + 
        //                   " shape: " + Utils.arrayToString(newShape) );
    
        ArraySpec returnSpec = null; 
        try {  
            returnSpec = new ArraySpec(normalizedGID, newShape, "_", "_"); 
        } catch ( Exception e ) {
            System.out.println("Caught an exception in GroupIDGen.getGIDArraySpec()");
        }

        return returnSpec;
    }

    /**
     * This should take an array, get the bounding GroupIDs 
     * @param myGIDG a GroupIDGen object
     * @param data the data covered by the group of produced IDs
     * @param spec the logical space to get GroupIDs for
     * @param extractionShape the shape to be tiled over the logical data space
     * @param ones helper array that is full of ones
     * @param returnMap a HashMap of GroupID to Array mappings. This carries the 
     * results of this function.
     */
    public static void pullOutSubArrays( GroupIDGen myGIDG, ArrayInt data, 
                                          ArraySpec spec, int[] extractionShape, 
                                          int[] ones,
                                          HashMap<GroupID, Array> returnMap) {

        LOG.info("pullOutSubArrays passed ArraySpec: " + spec.toString() + 
                 " ex shape: " + Utils.arrayToString(extractionShape));
        GroupID[] gids = myGIDG.getBoundingGroupIDs( spec, extractionShape);
        LOG.info("getBoundingGroupIDs: getBounding returned " + gids.length + " gids " + 
                 " for ArraySpec: " + spec.toString() + 
                 " ex shape: " + Utils.arrayToString(extractionShape));

        ArraySpec tempArraySpec;
        ArrayInt tempIntArray;
        int[] readCorner = new int[extractionShape.length];

        for( GroupID gid : gids ) {
            //System.out.println("gid: " + gid);

            tempArraySpec = getGIDArraySpec( spec, gid, extractionShape, ones);        
            try { 

                // note, the tempArraySpec is in the global space
                // need to translate that into the local space prior to pull out the subarray
                for( int i=0; i<readCorner.length; i++) {
                    readCorner[i] = tempArraySpec.getCorner()[i] - spec.getCorner()[i];
                }

                //System.out.println("\t\tlocal read corner: " + Utils.arrayToString(readCorner) );
                tempIntArray = (ArrayInt)data.sectionNoReduce(readCorner, tempArraySpec.getShape(), ones);

               /* 
                System.out.println("\tsubArray ( gid: " + gid.toString(extractionShape) + 
                               " ex shape: " + Utils.arrayToString(extractionShape) + ")" + 
                               " read corner: " + Utils.arrayToString(readCorner) + 
                               " read shape: " + Utils.arrayToString(tempArraySpec.getShape()) + 
                               "\n"); 
               */
                
                returnMap.put(gid, tempIntArray);

            } catch (InvalidRangeException ire) {
                System.out.println("Caught an ire in GroupIDGen.pullOutSubArrays()");
            }
        }

        return;
    }  
   
    /**
     * This is used for testing this class
     */ 
    public static void main(String[] args) {

        // let's add some testing harnesses here
      //System.out.println("Arg[0]: " + args[0]);         

      /*
      String inputFile = "";
      String extractionShape = "";

      if ( args[0].compareTo("-h") == 0 ) {
        System.out.println("groupIDGen.jar --inputFile <intputFile> --extractionShape=\"<extractionShape>\"");
        return;
      } else {
        for ( int i=0; i<args.length; i++) {
          if ( args[i].compareTo("--inputFile") == 0 ) {

          }
        }
      }
      */


        try { 
            GroupIDGen myGIDG = new GroupIDGen();

           /*  
            int[] extractionShape = { 2, 4, 2};

            int[] corner = {3,0,0};
            int[] shape = {8,4,4};
            int[] variableShape = {16,4,4};

            int[] ones = new int[corner.length];

            for( int i=0; i<ones.length; i++) {
                ones[i] = 1;
            }
            */
            
            
           /* 
            int[] extractionShape = { 1,360,360,50 };

            int[] corner = {2,0,0,0};
            int[] shape = {6,360,360,50};
            int[] variableShape = {5475,360,360,50};
            */
            
            /*
            int[] extractionShape = { 1,36,36,50 };

            int[] corner = {0,0,0,0};
            int[] shape = {5475,360,360,50};
            int[] variableShape = {5475,360,360,50};
            */

            // wrong order
            int[] variableShape = {10, 360, 360, 50};
            int[] extractionShape = {1, 360, 360, 50};
            int[] allZeroes = {0,0,0,0};

            System.out.println("exShape: " + Arrays.toString(extractionShape));
            System.out.println("exShape2: " + Utils.arrayToString(extractionShape));

            int[] ones = new int[variableShape.length];
            for( int i=0; i<ones.length; i++) {
                ones[i] = 1;
            }

            ArrayList<int[]> cornerList  = new ArrayList<int[]>();
            ArrayList<int[]> shapeList  = new ArrayList<int[]>();

            int[] tempCorner = new int[variableShape.length];
            int[] tempShape = new int[variableShape.length];

            tempCorner[0] = 0;
            tempCorner[1] = 0;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 2;
            tempShape[1] = 360;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 0;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 212;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 5;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 5;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 1;
            tempShape[3] = 45;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 5;
            tempCorner[3] = 45;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 1;
            tempShape[3] = 5;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 6;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 354;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 213;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 147;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 3;
            tempCorner[1] = 0;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 2;
            tempShape[1] = 360;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            //cornerList.add( {0,0,0,0} );
            //shapeList.add( {2, 360, 360, 50} );


           
            int[][] allCorners = new int[cornerList.size()][];
            allCorners = cornerList.toArray(allCorners);
            int[][] allShapes = new int[shapeList.size()][];
            allShapes = shapeList.toArray(allShapes);

            ArraySpec tempSpec = new ArraySpec();

            HashMap<GroupID, Array> returnMap = new HashMap<GroupID, Array>(); 
            for ( int i = 0; i < allCorners.length; i++ ) { 
              ArraySpec key = new ArraySpec(allCorners[i], allShapes[i], "var1","_", variableShape);

              System.out.println("inputSplit corner: " + Utils.arrayToString( allCorners[i] )  +
                                 " shape: " + Utils.arrayToString( allShapes[i] ) );

                ArrayInt values = populateArray(variableShape);

                ArrayInt subArray = 
                  (ArrayInt)values.sectionNoReduce(allCorners[i], allShapes[i], ones);
                pullOutSubArrays( myGIDG, subArray, key, extractionShape, ones, returnMap);
              }


        } catch ( Exception e ) {
            System.out.println("caught an exception in main() \n" + e.toString() );
        }

    }
}
