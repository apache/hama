package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

public interface InputSplit extends Writable {

  /**
   * Get the total number of bytes in the data of the <code>InputSplit</code>.
   * 
   * @return the number of bytes in the input split.
   * @throws IOException
   */
  long getLength() throws IOException;
  
  /**
   * Get the list of hostnames where the input split is located.
   * 
   * @return list of hostnames where data of the <code>InputSplit</code> is
   *         located as an array of <code>String</code>s.
   * @throws IOException
   */
  String[] getLocations() throws IOException;
}
