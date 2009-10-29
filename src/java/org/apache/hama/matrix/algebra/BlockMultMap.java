package org.apache.hama.matrix.algebra;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hama.Constants;
import org.apache.hama.io.BlockID;
import org.apache.hama.matrix.SubMatrix;

public class BlockMultMap extends TableMapper<BlockID, BytesWritable> {
  private byte[] COLUMN = Bytes.toBytes(Constants.BLOCK);
  
  public void map(ImmutableBytesWritable key, Result value, Context context) 
  throws IOException, InterruptedException {
    SubMatrix a = new SubMatrix(value.getValue(COLUMN, Bytes.toBytes("a")));
    SubMatrix b = new SubMatrix(value.getValue(COLUMN, Bytes.toBytes("b")));
    
    SubMatrix c = a.mult(b);
    context.write(new BlockID(key.get()), new BytesWritable(c.getBytes()));
  }
}
