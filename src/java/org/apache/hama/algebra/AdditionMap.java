package org.apache.hama.algebra;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Vector;
import org.apache.hama.io.VectorDatum;
import org.apache.hama.mapred.MatrixMap;

public class AdditionMap extends MatrixMap<ImmutableBytesWritable, VectorDatum> {

  public void map(ImmutableBytesWritable key, VectorDatum value,
      OutputCollector<ImmutableBytesWritable, VectorDatum> output,
      Reporter reporter) throws IOException {

    Vector v1 = new Vector(B.getRowResult(key.get()));
    output.collect(key, v1.addition(key.get(), value.getVector()));
  }

}
