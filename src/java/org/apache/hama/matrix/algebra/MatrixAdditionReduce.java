package org.apache.hama.matrix.algebra;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.util.BytesUtil;

public class MatrixAdditionReduce extends
    TableReducer<IntWritable, MapWritable, Writable> {

  @Override
  public void reduce(IntWritable key, Iterable<MapWritable> values,
      Context context) throws IOException, InterruptedException {

    Put put = new Put(BytesUtil.getRowIndex(key.get()));
    for (MapWritable value : values) {
      for (Map.Entry<Writable, Writable> e : value.entrySet()) {

        put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String
            .valueOf(((IntWritable) e.getKey()).get())), Bytes
            .toBytes(((DoubleWritable) e.getValue()).get()));
      }
    }

    context.write(new ImmutableBytesWritable(BytesUtil.getRowIndex(key.get())),
        put);
  }
}
