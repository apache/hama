package org.apache.hama.bsp;

import java.io.IOException;

public interface RecordWriter<K, V> {
  /**
   * Writes a key/value pair.
   * 
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException
   */
  void write(K key, V value) throws IOException;

  /**
   * Close this <code>RecordWriter</code> to future operations.
   * 
   * @param reporter facility to report progress.
   * @throws IOException
   */
  void close() throws IOException;
}
