package org.apache.hama.examples.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.examples.JacobiEigen;
import org.apache.hama.util.BytesUtil;

public class PivotInputFormat extends InputFormat<Pair, DoubleWritable>
    implements Configurable {
  final static Log LOG = LogFactory.getLog(PivotInputFormat.class);

  /** Job parameter that specifies the output table. */
  public static final String INPUT_TABLE = "hama.mapreduce.inputtable";
  /** Space delimited list of columns. */
  public static final String SCAN = "hama.mapreduce.scan";
  
  /** The configuration. */
  private Configuration conf = null;
  
  /** Holds the details for the internal scanner. */
  private Scan scan = null;
  /** The table to scan. */
  private HTable table = null;
  /** The reader scanning the table, can be a custom one. */
  private PivotRecordReader pivotRecordReader = null;

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    byte[][] startKeys = table.getStartKeys();
    if (startKeys == null || startKeys.length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    int realNumSplits = startKeys.length;
    InputSplit[] splits = new InputSplit[realNumSplits];
    int middle = startKeys.length / realNumSplits;
    int startPos = 0;
    for (int i = 0; i < realNumSplits; i++) {
      int lastPos = startPos + middle;
      lastPos = startKeys.length % realNumSplits > i ? lastPos + 1 : lastPos;
      String regionLocation = table.getRegionLocation(startKeys[startPos])
          .getServerAddress().getHostname();
      splits[i] = new TableSplit(this.table.getTableName(),
          startKeys[startPos], ((i + 1) < realNumSplits) ? startKeys[lastPos]
              : HConstants.EMPTY_START_ROW, regionLocation);
      LOG.info("split: " + i + "->" + splits[i]);
      startPos = lastPos;
    }
    return Arrays.asList(splits);
  }

  protected static class PivotRecordReader extends
      RecordReader<Pair, DoubleWritable> {
    private int totalRows;
    private int processedRows;
    private int size;
    boolean mocked = true;

    private ResultScanner scanner = null;
    private Scan scan = null;
    private HTable htable = null;
    private byte[] lastRow = null;
    private Pair key = null;
    private DoubleWritable value = null;

    @Override
    public void close() {
      this.scanner.close();
    }

    public void setScan(Scan scan) {
      this.scan = scan;
    }

    public void setHTable(HTable htable) {
      this.htable = htable;
    }

    public void init() throws IOException {
      restart(scan.getStartRow());
    }

    public void restart(byte[] firstRow) throws IOException {
      Scan newScan = new Scan(scan);
      newScan.setStartRow(firstRow);
      this.scanner = this.htable.getScanner(newScan);
    }

    @Override
    public Pair getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public DoubleWritable getCurrentValue() throws IOException,
        InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (totalRows <= 0) {
        return 0;
      } else {
        return Math.min(1.0f, processedRows / (float) totalRows);
      }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (key == null)
        key = new Pair();
      if (value == null)
        value = new DoubleWritable();

      Result vv;
      try {
        vv = this.scanner.next();
      } catch (IOException e) {
        LOG.debug("recovered from " + StringUtils.stringifyException(e));
        restart(lastRow);
        scanner.next(); // skip presumed already mapped row
        vv = scanner.next();
      }

      boolean hasMore = vv != null && vv.size() > 0;
      if (hasMore) {

        byte[] row = vv.getRow();

        int rowId = BytesUtil.getRowIndex(row);
        if (rowId == size - 1) { // skip the last row
          if (mocked) {
            key.set(Integer.MAX_VALUE, Integer.MAX_VALUE);
            mocked = false;
            return true;
          } else {
            return false;
          }
        }

        byte[] col = vv.getValue(Bytes
            .toBytes(JacobiEigen.EI), Bytes
            .toBytes(JacobiEigen.EIIND));
        int colId = BytesUtil.bytesToInt(col);
        double val = 0;

        Get get = new Get(BytesUtil.getRowIndex(rowId));
        byte[] cell = htable.get(get).getValue(
            Bytes.toBytes(JacobiEigen.EICOL),
            Bytes.toBytes(String.valueOf(colId)));
        if (cell != null) {
          val = Bytes.toDouble(cell);
        }

        key.set(rowId, colId);
        value.set(val);

        lastRow = row;
      } else {
        if (mocked) {
          key.set(Integer.MAX_VALUE, Integer.MAX_VALUE);
          mocked = false;
          return true;
        } else {
          return false;
        }
      }

      return hasMore;
    }
  }

  @Override
  public RecordReader<Pair, DoubleWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    TableSplit tSplit = (TableSplit) split;
    PivotRecordReader trr = this.pivotRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new PivotRecordReader();
    }
    Scan sc = new Scan(this.scan);
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setHTable(table);
    trr.init();
    return trr;
  }

  protected HTable getHTable() {
    return this.table;
  }

  protected void setHTable(HTable table) {
    this.table = table;
  }

  public Scan getScan() {
    if (this.scan == null)
      this.scan = new Scan();
    return scan;
  }

  public void setScan(Scan scan) {
    this.scan = scan;
  }

  protected void setTableRecordReader(PivotRecordReader pivotRecordReader) {
    this.pivotRecordReader = pivotRecordReader;
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    String tableName = conf.get(INPUT_TABLE);
    try {
      setHTable(new HTable(new HBaseConfiguration(conf), tableName));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    Scan scan = null;
    try {
      scan = convertStringToScan(conf.get(SCAN));
    } catch (IOException e) {
      LOG.error("An error occurred.", e);
    }
    setScan(scan);
  }
  
  public static String convertScanToString(Scan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();  
    DataOutputStream dos = new DataOutputStream(out);
    scan.write(dos);
    return Base64.encodeBytes(out.toByteArray());
  }
  
  public static Scan convertStringToScan(String base64) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
    DataInputStream dis = new DataInputStream(bis);
    Scan scan = new Scan();
    scan.readFields(dis);
    return scan;
  }
  
}
