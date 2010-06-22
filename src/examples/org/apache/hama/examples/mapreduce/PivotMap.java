package org.apache.hama.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PivotMap extends
    Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {
  private double max = 0;
  private Pair pair = new Pair(0, 0);
  private Pair dummyPair = new Pair(Integer.MAX_VALUE, Integer.MAX_VALUE);
  private DoubleWritable dummyVal = new DoubleWritable(0.0);

  public void map(Pair key, DoubleWritable value, Context context)
      throws IOException, InterruptedException {
    if (key.getRow() != Integer.MAX_VALUE) {
      if (Math.abs(value.get()) > Math.abs(max)) {
        pair.set(key.getRow(), key.getColumn());
        max = value.get();
      }
    } else {
      context.write(pair, new DoubleWritable(max));
      context.write(dummyPair, dummyVal);
    }
  }

}
