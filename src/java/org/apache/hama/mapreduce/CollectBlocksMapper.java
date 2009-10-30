package org.apache.hama.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hama.io.BlockID;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.util.BytesUtil;

public class CollectBlocksMapper extends TableMapper<BlockID, MapWritable>
    implements Configurable {
  private Configuration conf = null;
  /** Parameter of the path of the matrix to be blocked * */
  public static final String BLOCK_SIZE = "hama.blocking.size";
  public static final String ROWS = "hama.blocking.rows";
  public static final String COLUMNS = "hama.blocking.columns";
  public static final String MATRIX_POS = "a.or.b";

  private int mBlockNum;
  private int mBlockRowSize;
  private int mBlockColSize;
  private int mRows;
  private int mColumns;
  
  public void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    int startColumn, endColumn, blkRow = BytesUtil.getRowIndex(key.get())
        / mBlockRowSize, i = 0;
    DenseVector dv = new DenseVector(BytesUtil.getRowIndex(key.get()), value);

    do {
      startColumn = i * mBlockColSize;
      endColumn = startColumn + mBlockColSize - 1;
      if (endColumn >= mColumns) // the last sub vector
        endColumn = mColumns - 1;
      context.write(new BlockID(blkRow, i), dv.subVector(startColumn, endColumn).getEntries());

      i++;
    } while (endColumn < (mColumns - 1));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    mBlockNum = Integer.parseInt(conf.get(BLOCK_SIZE, ""));
    mRows = Integer.parseInt(conf.get(ROWS, ""));
    mColumns = Integer.parseInt(conf.get(COLUMNS, ""));

    mBlockRowSize = mRows / mBlockNum;
    mBlockColSize = mColumns / mBlockNum;
  }
}
