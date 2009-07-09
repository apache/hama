package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/** Implements the dummy function, mapping inputs directly to outputs. */
public class DummyMapper<K, V>
    extends MapReduceBase implements Mapper<K, V, K, V> {

  /** The dummy function. */
  public void map(K key, V val,
                  OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
    // do nothing
  }
}