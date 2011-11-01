package org.apache.hama.bsp;

import org.apache.hadoop.fs.FileSystem;

public class NullOutputFormat<K, V> implements OutputFormat<K, V> {

  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, BSPJob job,
      String name) {
    return new RecordWriter<K, V>() {
      public void write(K key, V value) {
      }

      public void close() {
      }
    };
  }

  public void checkOutputSpecs(FileSystem ignored, BSPJob job) {
  }
}
