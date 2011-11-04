package edu.ucsc.srl.damasc.netcdf.io;

import org.apache.hadoop.fs.FileStatus;

/**
 * This class represents a FileStatus an a logical offset. It's used
 * to map multiple files into a single logical space
 */
public class SHFileStatus implements Comparable<SHFileStatus> {
  private FileStatus _fileStatus;
  private int[] _startOffset;

  /**
   * Constructor
   * @param fileStatus the FileStatus to wrap
   * @param startOffset the offset, in the global logical space, where
   * this file starts
   */
  public SHFileStatus( FileStatus fileStatus, int[] startOffset) {
    this.setFileStatus( fileStatus);
    this.setStartOffset(startOffset);
  }

  /**
   * Sets the FileStatus object wrapped by this SHFileStatus object
   * @param newFileStatus a FileStatus object to wrap
   */
  public void setFileStatus( FileStatus newFileStatus ){
    _fileStatus = newFileStatus;
  }
  
  /**
   * Sets the offset, in the global space, for this SHFileStatus object
   * @param newStartOffset an offset in the global logical space
   */
  public void setStartOffset(int[] newStartOffset) {
    _startOffset = new int[newStartOffset.length];

    for ( int i=0; i < newStartOffset.length; i++) {
      _startOffset[i] = newStartOffset[i];
    }

  }  

  /**
   * Returns the offset, in the global space, for this SHFileStatus object
   * @return the offset, in the global logical space, for this SHFileStatus 
   * object
   */
  public int[] getStartOffset() {
    return _startOffset;
  }

  /**
   * Gets the FileStatus object wrapped by this SHFileStatus object
   * @return  the FileStatus object wrapped by this SHFileStatus object
   */
  public FileStatus getFileStatus() {
    return _fileStatus;
  }

  /** 
   * Compare this SHFileStatus object to another.
   * Use the encapsulated FileStatus compareTo method
   * @param other the SHFileStatus object to compare to this one
   * @return a value that is less than, equal to, or greater than zero
   * if the object passed in is, respectively, less than, equal to, or 
   * greater than this object
   */
  public int compareTo( SHFileStatus other ) {
    return this._fileStatus.compareTo(other.getFileStatus());
  }

}

