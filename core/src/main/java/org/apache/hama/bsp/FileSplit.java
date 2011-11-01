package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class FileSplit implements InputSplit {
  private Path file;
  private long start;
  private long length;
  private String[] hosts;

  FileSplit() {
  }

  /**
   * Constructs a split.
   * 
   * @deprecated
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   */
  public FileSplit(Path file, long start, long length, BSPJob conf) {
    this(file, start, length, (String[]) null);
  }

  /**
   * Constructs a split with host information
   * 
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }

  /** The file containing this split's data. */
  public Path getPath() {
    return file;
  }

  /** The position of the first byte in the file to process. */
  public long getStart() {
    return start;
  }

  /** The number of bytes in the file to process. */
  public long getLength() {
    return length;
  }

  public String toString() {
    return file + ":" + start + "+" + length;
  }

  // //////////////////////////////////////////
  // Writable methods
  // //////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
  }

  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    hosts = null;
  }

  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[] {};
    } else {
      return this.hosts;
    }
  }
}
