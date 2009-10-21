package org.apache.hama.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

public class RandomMatrixReducer extends
    TableReducer<IntWritable, MapWritable, Writable> {
  static final Logger LOG = Logger.getLogger(RandomMatrixReducer.class);

  public void reduce(IntWritable key, Iterable<MapWritable> values,
      Context context) throws IOException, InterruptedException {
    VectorUpdate update = new VectorUpdate(key.get());
    update.putAll(values.iterator().next());
    context.write(new ImmutableBytesWritable(BytesUtil.getRowIndex(key.get())),
        update.getPut());
  }
}
