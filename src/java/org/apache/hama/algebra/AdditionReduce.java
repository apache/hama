package org.apache.hama.algebra;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorDatum;
import org.apache.hama.mapred.MatrixReduce;

public class AdditionReduce extends
    MatrixReduce<ImmutableBytesWritable, VectorDatum> {

  @Override
  public void reduce(ImmutableBytesWritable key, Iterator<VectorDatum> values,
      OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
      Reporter reporter) throws IOException {

    BatchUpdate b = new BatchUpdate(key.get());
    VectorDatum vector = values.next();
    for (Map.Entry<byte[], Cell> f : vector.entrySet()) {
      b.put(f.getKey(), f.getValue().getValue());
    }

    output.collect(key, b);
  }

}
