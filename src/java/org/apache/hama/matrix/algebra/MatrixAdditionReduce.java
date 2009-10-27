package org.apache.hama.matrix.algebra;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.BytesUtil;

public class MatrixAdditionReduce extends
TableReducer<IntWritable, MapWritable, Writable> {
  
  @Override
  public void reduce(IntWritable key, Iterable<MapWritable> values,
      Context context) throws IOException, InterruptedException {

    VectorUpdate update = new VectorUpdate(key.get());
    for (MapWritable value : values) {
      update.putAll(value);
    }
    
    context.write(new ImmutableBytesWritable(BytesUtil.getRowIndex(key.get())),
        update.getPut());
  }
}
