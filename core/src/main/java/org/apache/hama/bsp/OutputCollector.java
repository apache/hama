package org.apache.hama.bsp;

import java.io.IOException;

public interface OutputCollector<K, V> {

  /**
   * Adds a key/value pair to the output.
   * 
   * @param key the key to collect.
   * @param value to value to collect.
   * @throws IOException
   */
  void collect(K key, V value) throws IOException;
}
