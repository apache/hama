package org.apache.hama.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.io.BlockID;
import org.apache.hama.matrix.SubMatrix;
import org.apache.hama.util.BytesUtil;

public class BlockMultReduce extends
    TableReducer<BlockID, BytesWritable, Writable> {
  
  @Override
  public void reduce(BlockID key, Iterable<BytesWritable> values,
      Context context) throws IOException, InterruptedException {
    SubMatrix s = null;
    for (BytesWritable value : values) {
      SubMatrix b = new SubMatrix(value.getBytes());
      if (s == null) {
        s = b;
      } else {
        s = s.add(b);
      }
    }

    int startRow = key.getRow() * s.getRows();
    int startColumn = key.getColumn() * s.getColumns();

    for (int i = 0; i < s.getRows(); i++) {
      Put put = new Put(BytesUtil.getRowIndex(i + startRow));
      for (int j = 0; j < s.getColumns(); j++) {
        put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j + startColumn)),
            Bytes.toBytes(s.get(i, j)));
      }

      context.write(new ImmutableBytesWritable(BytesUtil.getRowIndex(key
          .getRow())), put);
    }
  }
}
