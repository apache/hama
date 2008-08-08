package org.apache.hama.algebra;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Vector;
import org.apache.hama.mapred.MatrixMap;

public class AdditionMap extends MatrixMap<ImmutableBytesWritable, Vector> {

  public void map(ImmutableBytesWritable key, Vector value,
      OutputCollector<ImmutableBytesWritable, Vector> output,
      Reporter reporter) throws IOException {

    Vector v1 = new Vector(key.get(), B);
    output.collect(key, v1.add(value));
  }

}
