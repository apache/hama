package org.apache.hama.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class DummyMapper<K, V> extends Mapper<K, V, K, V> {
  /** The dummy function. */
  public void map(K key, V val, OutputCollector<K, V> output, Reporter reporter)
      throws IOException {
    // do nothing
  }
}
