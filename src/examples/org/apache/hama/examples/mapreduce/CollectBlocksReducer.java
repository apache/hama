package org.apache.hama.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.matrix.SubMatrix;

public class CollectBlocksReducer extends
    TableReducer<BlockID, MapWritable, Writable> implements Configurable {
  private Configuration conf = null;
  private int mBlockNum;
  private int mBlockRowSize;
  private int mBlockColSize;
  private int mRows;
  private int mColumns;
  private boolean matrixPos;
  
  public void reduce(BlockID key, Iterable<MapWritable> values,
      Context context) throws IOException, InterruptedException {
    // the block's base offset in the original matrix
    int colBase = key.getColumn() * mBlockColSize;
    int rowBase = key.getRow() * mBlockRowSize;

    // the block's size : rows & columns
    int smRows = mBlockRowSize;
    if ((rowBase + mBlockRowSize - 1) >= mRows)
      smRows = mRows - rowBase;
    int smCols = mBlockColSize;
    if ((colBase + mBlockColSize - 1) >= mColumns)
      smCols = mColumns - colBase;

    // construct the matrix
    SubMatrix subMatrix = new SubMatrix(smRows, smCols);
    // i, j is the current offset in the sub-matrix
    int i = 0, j = 0;
    for (MapWritable value : values) {
      DenseVector vw = new DenseVector(value);
      // check the size is suitable
      if (vw.size() != smCols)
        throw new IOException("Block Column Size dismatched.");
      i = vw.getRow() - rowBase;
      
      if (i >= smRows || i < 0)
        throw new IOException("Block Row Size dismatched.");

      // put the subVector to the subMatrix
      for (j = 0; j < smCols; j++) {
        subMatrix.set(i, j, vw.get(colBase + j));
      }
    }
    //BlockWritable outValue = new BlockWritable(subMatrix);
    
    // It'll used for only matrix multiplication.
    if (matrixPos) {
      for (int x = 0; x < mBlockNum; x++) {
        int r = (key.getRow() * mBlockNum) * mBlockNum;
        int seq = (x * mBlockNum) + key.getColumn() + r;
        BlockID bkID = new BlockID(key.getRow(), x, seq);
        Put put = new Put(bkID.getBytes());
        put.add(Bytes.toBytes(Constants.BLOCK), 
            Bytes.toBytes("a"), 
            subMatrix.getBytes());
        context.write(new ImmutableBytesWritable(bkID.getBytes()), put);
      }
    } else {
      for (int x = 0; x < mBlockNum; x++) {
        int seq = (x * mBlockNum * mBlockNum) + (key.getColumn() * mBlockNum)
            + key.getRow();
        BlockID bkID = new BlockID(x, key.getColumn(), seq);
        Put put = new Put(bkID.getBytes());
        put.add(Bytes.toBytes(Constants.BLOCK), 
            Bytes.toBytes("b"), 
            subMatrix.getBytes());
        context.write(new ImmutableBytesWritable(bkID.getBytes()), put);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    mBlockNum = Integer.parseInt(conf.get(CollectBlocksMapper.BLOCK_SIZE, ""));
    mRows = Integer.parseInt(conf.get(CollectBlocksMapper.ROWS, ""));
    mColumns = Integer.parseInt(conf.get(CollectBlocksMapper.COLUMNS, ""));

    mBlockRowSize = mRows / mBlockNum;
    mBlockColSize = mColumns / mBlockNum;

    matrixPos = conf.getBoolean(CollectBlocksMapper.MATRIX_POS, true);
  }
}
