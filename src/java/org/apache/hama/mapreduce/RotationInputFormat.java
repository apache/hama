package org.apache.hama.mapreduce;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.Constants;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.BytesUtil;

public class RotationInputFormat extends
    InputFormat<NullWritable, NullWritable> implements Configurable {
  final static Log LOG = LogFactory.getLog(RotationInputFormat.class);
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
  private RotationRecordReader rotationRecordReader;

  int pivot_row, pivot_col;
  double pivot_cos, pivot_sin;

  protected static class RotationRecordReader extends
      RecordReader<NullWritable, NullWritable> {
    private ResultScanner scanner = null;
    private Scan scan = null;
    private HTable htable = null;
    private byte[] lastRow = null;

    private int totalRows;
    private int processedRows;
    int startRowId, endRowId = -1;
    int size;

    int pivotrow, pivotcol;
    byte[] prow, pcol;
    double pivotcos, pivotsin;

    public RotationRecordReader(int pr, int pc, double psin, double pcos) {
      super();
      pivotrow = pr;
      pivotcol = pc;
      pivotsin = psin;
      pivotcos = pcos;
      prow = Bytes.toBytes(pivotrow);
      pcol = Bytes.toBytes(pivotcol);
      LOG.info(prow);
      LOG.info(pcol);
    }

    public void setScan(Scan scan) {
      this.scan = scan;
    }

    public void setHTable(HTable htable) {
      this.htable = htable;
    }

    public void init() throws IOException {
      restart(scan.getStartRow());
      byte[] startRow = scan.getStartRow();
      byte[] endRow = scan.getStopRow();

      Get get = new Get(Bytes.toBytes(Constants.METADATA));
      get.addFamily(Constants.ATTRIBUTE);
      byte[] result = htable.get(get).getValue(Constants.ATTRIBUTE,
          Bytes.toBytes("rows"));

      size = (result != null) ? Bytes.toInt(result) : 0;

      if (endRow.length == 0) { // the last split, we don't know the end row
        totalRows = 0; // so we just skip it.
        if (startRow.length == 0)
          startRowId = 0;
        else
          startRowId = BytesUtil.getRowIndex(startRow);
        endRowId = -1;
      } else {
        if (startRow.length == 0) { // the first split, start row is 0
          totalRows = BytesUtil.getRowIndex(endRow);
          startRowId = 0;
          endRowId = totalRows;
        } else {
          startRowId = BytesUtil.getRowIndex(startRow);
          endRowId = BytesUtil.getRowIndex(endRow);
          totalRows = startRowId - endRowId;
        }
      }
      processedRows = 0;
      LOG.info("Split (" + startRowId + ", " + endRowId + ") -> " + totalRows);
    }

    public void restart(byte[] firstRow) throws IOException {
      Scan newScan = new Scan(scan);
      newScan.setStartRow(firstRow);
      this.scanner = this.htable.getScanner(newScan);
    }

    @Override
    public void close() throws IOException {
      this.scanner.close();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException,
        InterruptedException {
      return NullWritable.get();
    }

    @Override
    public NullWritable getCurrentValue() throws IOException,
        InterruptedException {
      return NullWritable.get();
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
      Result vv;
      try {
        vv = this.scanner.next();
      } catch (IOException e) {
        LOG.debug("recovered from " + StringUtils.stringifyException(e));
        restart(lastRow);
        scanner.next(); // skip presumed already mapped row
        vv = scanner.next();
      }

      double s1, s2;
      VectorUpdate bu;
      boolean hasMore = vv != null && vv.size() > 0;
      if (hasMore) {
        byte[] row = vv.getRow();
        int rowId = BytesUtil.getRowIndex(row);
        if (rowId < pivotrow) {
          Get get = new Get(BytesUtil.getRowIndex(rowId));
          s1 = Bytes.toDouble(htable.get(get).getValue(
              Bytes.toBytes(Constants.EICOL),
              Bytes.toBytes(String.valueOf(pivotrow))));
          s2 = Bytes.toDouble(htable.get(get).getValue(
              Bytes.toBytes(Constants.EICOL),
              Bytes.toBytes(String.valueOf(pivotcol))));

          bu = new VectorUpdate(rowId);
          bu.put(Constants.EICOL, pivotrow, pivotcos * s1
              - pivotsin * s2);
          bu.put(Constants.EICOL, pivotcol, pivotsin * s1
              + pivotcos * s2);

          htable.put(bu.getPut());
        } else if (rowId == pivotrow) {
          return true;
        } else if (rowId < pivotcol) {
          Get get = new Get(BytesUtil.getRowIndex(pivotrow));
          s1 = Bytes.toDouble(htable.get(get).getValue(
              Bytes.toBytes(Constants.EICOL),
              Bytes.toBytes(String.valueOf(rowId))));
          get = new Get(BytesUtil.getRowIndex(rowId));

          s2 = Bytes.toDouble(htable.get(get).getValue(
              Bytes.toBytes(Constants.EICOL),
              Bytes.toBytes(String.valueOf(pivotcol))));

          bu = new VectorUpdate(rowId);
          bu.put(Constants.EICOL, pivotcol, pivotsin * s1
              + pivotcos * s2);
          htable.put(bu.getPut());

          bu = new VectorUpdate(pivotrow);
          bu.put(Constants.EICOL, rowId, pivotcos * s1 - pivotsin
              * s2);
          htable.put(bu.getPut());

        } else if (rowId == pivotcol) {
          for (int i = pivotcol + 1; i < size; i++) {
            Get get = new Get(BytesUtil.getRowIndex(pivotrow));

            s1 = Bytes.toDouble(htable.get(get).getValue(
                Bytes.toBytes(Constants.EICOL),
                Bytes.toBytes(String.valueOf(i))));

            get = new Get(BytesUtil.getRowIndex(pivotcol));
            s2 = Bytes.toDouble(htable.get(get).getValue(
                Bytes.toBytes(Constants.EICOL),
                Bytes.toBytes(String.valueOf(i))));

            bu = new VectorUpdate(pivotcol);
            bu.put(Constants.EICOL, i, pivotsin * s1 + pivotcos
                * s2);
            htable.put(bu.getPut());

            bu = new VectorUpdate(pivotrow);
            bu.put(Constants.EICOL, i, pivotcos * s1 - pivotsin
                * s2);
            htable.put(bu.getPut());
          }
        } else { // rowId > pivotcol
          return false;
        }

        lastRow = row;
      }
      return hasMore;
    }

  }

  @Override
  public RecordReader<NullWritable, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    TableSplit tSplit = (TableSplit) split;
    RotationRecordReader trr = this.rotationRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new RotationRecordReader(pivot_row, pivot_col, pivot_sin, pivot_cos);
    }
    Scan sc = new Scan(this.scan);
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setHTable(table);
    trr.init();
    return trr;
  }

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

  protected void setTableRecordReader(RotationRecordReader rotationRecordReader) {
    this.rotationRecordReader = rotationRecordReader;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    pivot_row = conf.getInt(Constants.PIVOTROW, -1);
    pivot_col = conf.getInt(Constants.PIVOTCOL, -1);
    pivot_sin = Double.parseDouble(conf.get(Constants.PIVOTSIN));
    pivot_cos = Double.parseDouble(conf.get(Constants.PIVOTCOS));

    this.conf = conf;
    String tableName = conf.get(INPUT_TABLE);
    try {
      setHTable(new HTable(new HBaseConfiguration(conf), tableName));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    Scan scan = null;
    try {
      scan = PivotInputFormat.convertStringToScan(conf.get(SCAN));
    } catch (IOException e) {
      LOG.error("An error occurred.", e);
    }
    setScan(scan);
  }

}
