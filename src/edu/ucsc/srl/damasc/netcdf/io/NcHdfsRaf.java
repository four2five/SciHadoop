package edu.ucsc.srl.damasc.netcdf.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ucar.unidata.io.RandomAccessFile;

/**
 * This class is a shim to allow the java NetCDF library
 * to read from HDFS. The main thing is that it does a seek
 * prior to every read.
 */
public class NcHdfsRaf extends RandomAccessFile {

  private FSDataInputStream _hdfs_file = null;
  private long total_length = 0;
  private FileSystem _fs = null;

  /**
   * Constructer that allows the NetCDF buffer size to be set
   * @param fileStatus a FileStatus object representing the file
   * to be opened
   * @param bufferSize the size of buffer that NetCDF should use
   * @param job a Configuration object containing information about the
   * currently executing job
   */
  public NcHdfsRaf(FileStatus fileStatus, Configuration job, int bufferSize) 
                   throws IOException {

    /* No file specified - use default buffer size in NetCDF RAF */
    super(bufferSize);

    /* Get input HDFS input stream */
    Path path = fileStatus.getPath();
    location = path.toString();
    total_length = fileStatus.getLen();
    openFiles.add(location);

    this._fs = path.getFileSystem(job);
    this._hdfs_file = this._fs.open(path);

  }

  /**
   * Constructor
   * @param fileStatus a FileStatus object representing the file to open
   * @param job a Configuration object containing information about the
   * currently executing job
   */
  public NcHdfsRaf(FileStatus fileStatus, Configuration job) 
                   throws IOException {

    this(fileStatus, job, defaultBufferSize);
  }

/*

  public NcHdfsRaf(FileStatus fileStatus, Configuration job) 
                   throws IOException {

    // No file specified - use default buffer size in NetCDF RAF 
    super(defaultBufferSize);

    // Get input HDFS input stream 
    Path path = fileStatus.getPath();
    location = path.toString();
    total_length = fileStatus.getLen();
    openFiles.add(location);

    this._fs = path.getFileSystem(job);
    this._hdfs_file = this._fs.open(path);

  }
  */
  

  /**
   * Wraps a read command to use the contained HDFS file
   * @param pos the position in the file to read from
   * @param buf the buffer to place the read data into
   * @param offset the offset in the file to read the data from
   * @param len the desired number of bytes to read
   * @return the number of bytes actually read
   */
  @Override
  protected int read_(long pos, byte[] buf, int offset, int len) 
                      throws IOException {
    int n = this._hdfs_file.read(pos, buf, offset, len);
    return n;
  }

  /**
   * Reads data from the file wrapped by this object 
   * and writes it to a ByteChannel object
   * @param dest the ByteChannel object to write the data to
   * @param offset the offset in the file to read from
   * @param nbytes the desired number of bytes to read from the file
   * @return the number of bytes actually read
   */
  @Override
  public long readToByteChannel(WritableByteChannel dest, long offset, 
                                long nbytes) 
                                throws IOException {
    int n = (int)nbytes;
    byte[] buf = new byte[n];
    int done = read_(offset, buf, 0, n);
    dest.write(ByteBuffer.wrap(buf));
    return done;
  }

  /**
   * Returns the length of the file wrapped by this object
   * @return a long value indicating the size of the file
   */
  @Override
  public long length() throws IOException {
    if (total_length < dataEnd)
      return dataEnd;
    else
      return total_length;
  }

  /**
   * Closes the files wrapped by this object
   */
  @Override 
  public void close() throws IOException{
    this._hdfs_file.close();
    super.close();
  }
}
