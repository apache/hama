package org.apache.hama.mapred;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Iterate over an HBase table data, return (IntWritable, MapWritable<Text,
 * ImmutableBytesWritable>) pairs
 */
public abstract class MatrixRecordReader implements RecordReader<HStoreKey, MapWritable> {
  protected final HScannerInterface m_scanner;
  protected final SortedMap<Text, byte[]> m_row = new TreeMap<Text, byte[]>();

  /**
   * Constructor
   * 
   * @param startRow start row of block
   * @param endRow end row of block
   * @param table HTable object
   * @param cols column specification
   * @throws IOException
   */
  public MatrixRecordReader(Text startRow, Text endRow, HTable table,
      Text[] cols) throws IOException {
    if (endRow != null && endRow.getLength() > 0) {
      this.m_scanner = table.obtainScanner(cols, startRow, endRow);
    } else {
      this.m_scanner = table.obtainScanner(cols, startRow);
    }
  }

  public void close() throws IOException {
    this.m_scanner.close();
  }

  /**
   * @return HStoreKey
   * 
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public HStoreKey createKey() {
    return new HStoreKey();
  }

  /**
   * @return MapWritable
   * 
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  @SuppressWarnings("unchecked")
  public MapWritable createValue() {
    return new MapWritable();
  }

  /** {@inheritDoc} */
  public long getPos() {
    return 0;
  }

  /** {@inheritDoc} */
  public float getProgress() {
    return 0;
  }
}
