package org.apache.hama.matrix.algebra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.Constants;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.io.Pair;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.HTableInputFormatBase;
import org.apache.hama.mapred.HTableRecordReaderBase;
import org.apache.hama.util.BytesUtil;

/**
 * A catalog class collect all the m/r classes to compute the matrix's eigen
 * values
 */
public class JacobiEigenValue {

  /** a matrix copy of the original copy collected in "eicol" family * */
  public static final String EICOL = "eicol:";
  /** a column family collect all values and statuses used during computation * */
  @Deprecated
  public static final String EI = "eival:";
  /** a column collect all the eigen values * */
  @Deprecated
  public static final String EIVAL = EI + "value";
  
  public static final String EI_COLUMNFAMILY = "eival";
  public static final String EI_VAL = "value";
  
  
  /** a column identify whether the eigen values have been changed * */
  public static final String EICHANGED = EI + "changed";
  /** a column identify the index of the max absolute value each row * */
  public static final String EIIND = EI + "ind";
  /** a matrix collect all the eigen vectors * */
  public static final String EIVEC = "eivec:";
  public static final String MATRIX = "hama.jacobieigenvalue.matrix";
  /** parameters for pivot * */
  public static final String PIVOTROW = "hama.jacobi.pivot.row";
  public static final String PIVOTCOL = "hama.jacobi.pivot.col";
  public static final String PIVOTSIN = "hama.jacobi.pivot.sin";
  public static final String PIVOTCOS = "hama.jacobi.pivot.cos";

  static final Log LOG = LogFactory.getLog(JacobiEigenValue.class);

  /**
   * The matrix will be modified during computing eigen value. So a new matrix
   * will be created to prevent the original matrix being modified. To reduce
   * the network transfer, we copy the "column" family in the original matrix to
   * a "eicol" family. All the following modification will be done over "eicol"
   * family.
   * 
   * And the output Eigen Vector Arrays "eivec", and the output eigen value
   * array "eival:value", and the temp status array "eival:changed", "eival:ind"
   * will be created.
   * 
   * Also "eival:state" will record the state of the rotation state of a matrix
   */
  public static class InitMapper extends MapReduceBase implements
      Mapper<IntWritable, MapWritable, NullWritable, NullWritable> {

    HTable table;

    @Override
    public void configure(JobConf job) {
      String tableName = job.get(MATRIX, "");
      try {
        table = new HTable(new HBaseConfiguration(job), tableName);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<NullWritable, NullWritable> collector, Reporter reporter)
        throws IOException {
      int row, col;
      row = key.get();
      VectorUpdate vu = new VectorUpdate(row);

      double val;
      double maxVal = Double.MIN_VALUE;
      int maxInd = row + 1;

      boolean init = true;
      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        val = ((DoubleEntry) e.getValue()).getValue();
        col = ((IntWritable) e.getKey()).get();
        // copy the original matrix to "EICOL" family
        vu.put(JacobiEigenValue.EICOL, col, val);
        // make the "EIVEC" a dialog matrix
        vu.put(JacobiEigenValue.EIVEC, col, col == row ? 1 : 0);
        if (col == row) {
          vu.put(JacobiEigenValue.EIVAL, val);
        }
        // find the max index
        if (col > row) {
          if (init) {
            maxInd = col;
            maxVal = val;
            init = false;
          } else {
            if (Math.abs(val) > Math.abs(maxVal)) {
              maxVal = val;
              maxInd = col;
            }
          }
        }
      }
      // index array
      vu.put(JacobiEigenValue.EIIND, maxInd);
      // Changed Array set to be true during initialization
      vu.put(JacobiEigenValue.EICHANGED, 1);

      table.commit(vu.getBatchUpdate());
    }

  }

  /**
   * PivotInputFormat & PivotMapper & PivotReducer are used to find the pivot in
   * a matrix
   */
  public static class PivotInputFormat extends HTableInputFormatBase implements
      InputFormat<Pair, DoubleWritable>, JobConfigurable {

    private PivotRecordReader tableRecordReader;

    protected static class PivotRecordReader extends HTableRecordReaderBase
        implements RecordReader<Pair, DoubleWritable> {

      private int totalRows;
      private int processedRows;
      private int size;
      boolean mocked = true;

      @Override
      public void init() throws IOException {
        super.init();

        Get get = new Get(Bytes.toBytes(Constants.METADATA));
        get.addFamily(Bytes.toBytes(Constants.ATTRIBUTE));
        byte[] result = htable.get(get).getValue(Bytes.toBytes(Constants.ATTRIBUTE), 
            Bytes.toBytes("rows"));
        size = (result != null) ? BytesUtil.bytesToInt(result) : 0;

        if (endRow.length == 0) { // the last split, we don't know the end row
          totalRows = 0; // so we just skip it.
        } else {
          if (startRow.length == 0) { // the first split, start row is 0
            totalRows = BytesUtil.bytesToInt(endRow);
          } else {
            totalRows = BytesUtil.bytesToInt(endRow)
                - BytesUtil.bytesToInt(startRow);
          }
        }
        processedRows = 0;
        LOG.info("Split (" + Bytes.toString(startRow) + ", "
            + Bytes.toString(endRow) + ") -> " + totalRows);
      }

      /**
       * @return Pair
       * 
       * @see org.apache.hadoop.mapred.RecordReader#createKey()
       */
      public Pair createKey() {
        return new Pair();
      }

      /**
       * @return DoubleWritable
       * 
       * @see org.apache.hadoop.mapred.RecordReader#createValue()
       */
      public DoubleWritable createValue() {
        return new DoubleWritable();
      }

      /**
       * @param key Pair as input key.
       * @param value DoubleWritable as input value
       * 
       * Converts Scanner.next() to Pair, DoubleWritable
       * 
       * @return true if there was more data
       * @throws IOException
       */
      public boolean next(Pair key, DoubleWritable value) throws IOException {
        RowResult result;
        try {
          result = this.scanner.next();
        } catch (UnknownScannerException e) {
          LOG.debug("recovered from " + StringUtils.stringifyException(e));
          restart(lastRow);
          this.scanner.next(); // skip presumed already mapped row
          result = this.scanner.next();
        }

        boolean hasMore = result != null && result.size() > 0;
        if (hasMore) {
          byte[] row = result.getRow();
          int rowId = BytesUtil.bytesToInt(row);
          if (rowId == size - 1) { // skip the last row
            if (mocked) {
              key.set(Integer.MAX_VALUE, Integer.MAX_VALUE);
              mocked = false;
              return true;
            } else {
              return false;
            }
          }

          byte[] col = result.get(EIIND).getValue();
          int colId = BytesUtil.bytesToInt(col);
          double val = 0;

          // get (rowId, colId)'s value
          Cell cell = htable.get(BytesUtil.getRowIndex(rowId), Bytes
              .toBytes(EICOL + colId));
          if (cell != null && cell.getValue() != null) {
            val = BytesUtil.bytesToDouble(cell.getValue());
          }

          key.set(rowId, colId);
          value.set(val);

          lastRow = row;
          processedRows++;
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

      @Override
      public float getProgress() {
        if (totalRows <= 0) {
          return 0;
        } else {
          return Math.min(1.0f, processedRows / (float) totalRows);
        }
      }

    }

    @Override
    public RecordReader<Pair, DoubleWritable> getRecordReader(InputSplit split,
        JobConf conf, Reporter reporter) throws IOException {
      TableSplit tSplit = (TableSplit) split;
      PivotRecordReader trr = this.tableRecordReader;
      // if no table record reader was provided use default
      if (trr == null) {
        trr = new PivotRecordReader();
      }
      trr.setStartRow(tSplit.getStartRow());
      trr.setEndRow(tSplit.getEndRow());
      trr.setHTable(this.table);
      trr.setInputColumns(this.inputColumns);
      trr.setRowFilter(this.rowFilter);
      trr.init();
      return trr;
    }

    protected void setTableRecordReader(PivotRecordReader tableRecordReader) {
      this.tableRecordReader = tableRecordReader;
    }

  }

  // find the pivot of the matrix
  public static class PivotMapper extends MapReduceBase implements
      Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {

    private double max = 0;
    private Pair pair = new Pair(0, 0);
    private Pair dummyPair = new Pair(Integer.MAX_VALUE, Integer.MAX_VALUE);
    private DoubleWritable dummyVal = new DoubleWritable(0.0);

    @Override
    public void map(Pair key, DoubleWritable value,
        OutputCollector<Pair, DoubleWritable> collector, Reporter reporter)
        throws IOException {
      if (key.getRow() != Integer.MAX_VALUE) {
        if (Math.abs(value.get()) > Math.abs(max)) {
          pair.set(key.getRow(), key.getColumn());
          max = value.get();
        }
      } else {
        collector.collect(pair, new DoubleWritable(max));
        collector.collect(dummyPair, dummyVal);
      }
    }

  }

  public static class PivotReducer extends MapReduceBase implements
      Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {

    private double max = 0;
    private Pair pair = new Pair(0, 0);

    @Override
    public void reduce(Pair key, Iterator<DoubleWritable> values,
        OutputCollector<Pair, DoubleWritable> collector, Reporter reporter)
        throws IOException {
      double val;
      if (key.getRow() != Integer.MAX_VALUE) {
        val = values.next().get();
        if (Math.abs(val) > Math.abs(max)) {
          pair.set(key.getRow(), key.getColumn());
          max = val;
        }
      } else {
        collector.collect(pair, new DoubleWritable(max));
      }
    }

  }

  /**
   * Tricky here! we rotation the matrix during we scan the matrix and update to
   * the matrix so we just need a rotationrecordreader to scan the matrix and do
   * the rotation the mapper&reducer just a dummy mapper
   */
  public static class RotationInputFormat extends HTableInputFormatBase
      implements InputFormat<NullWritable, NullWritable>, JobConfigurable {

    private RotationRecordReader tableRecordReader;

    int pivot_row, pivot_col;
    double pivot_cos, pivot_sin;

    public void configure(JobConf job) {
      super.configure(job);
      pivot_row = job.getInt(PIVOTROW, -1);
      pivot_col = job.getInt(PIVOTCOL, -1);
      pivot_sin = Double.parseDouble(job.get(PIVOTSIN));
      pivot_cos = Double.parseDouble(job.get(PIVOTCOS));
    }

    protected static class RotationRecordReader extends HTableRecordReaderBase
        implements RecordReader<NullWritable, NullWritable> {

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

      @Override
      public void init() throws IOException {
        super.init();

        Get get = new Get(Bytes.toBytes(Constants.METADATA));
        get.addFamily(Bytes.toBytes(Constants.ATTRIBUTE));
        byte[] result = htable.get(get).getValue(Bytes.toBytes(Constants.ATTRIBUTE), 
            Bytes.toBytes("rows"));
        
        size = (result != null) ? BytesUtil.bytesToInt(result) : 0;

        if (endRow.length == 0) { // the last split, we don't know the end row
          totalRows = 0; // so we just skip it.
          if (startRow.length == 0)
            startRowId = 0;
          else
            startRowId = BytesUtil.bytesToInt(startRow);
          endRowId = -1;
        } else {
          if (startRow.length == 0) { // the first split, start row is 0
            totalRows = BytesUtil.bytesToInt(endRow);
            startRowId = 0;
            endRowId = totalRows;
          } else {
            startRowId = BytesUtil.bytesToInt(startRow);
            endRowId = BytesUtil.bytesToInt(endRow);
            totalRows = startRowId - endRowId;
          }
        }
        processedRows = 0;
        LOG
            .info("Split (" + startRowId + ", " + endRowId + ") -> "
                + totalRows);
      }

      /**
       * @return NullWritable
       * 
       * @see org.apache.hadoop.mapred.RecordReader#createKey()
       */
      public NullWritable createKey() {
        return NullWritable.get();
      }

      /**
       * @return NullWritable
       * 
       * @see org.apache.hadoop.mapred.RecordReader#createValue()
       */
      public NullWritable createValue() {
        return NullWritable.get();
      }

      /**
       * @param key NullWritable as input key.
       * @param value NullWritable as input value
       * 
       * Converts Scanner.next() to NullWritable, NullWritable
       * 
       * @return true if there was more data
       * @throws IOException
       */
      public boolean next(NullWritable key, NullWritable value)
          throws IOException {
        RowResult result;
        try {
          result = this.scanner.next();
        } catch (UnknownScannerException e) {
          LOG.debug("recovered from " + StringUtils.stringifyException(e));
          restart(lastRow);
          this.scanner.next(); // skip presumed already mapped row
          result = this.scanner.next();
        }

        double s1, s2;
        VectorUpdate bu;
        boolean hasMore = result != null && result.size() > 0;
        if (hasMore) {
          byte[] row = result.getRow();
          int rowId = BytesUtil.bytesToInt(row);
          if (rowId < pivotrow) {
            s1 = BytesUtil.bytesToDouble(htable.get(
                BytesUtil.getRowIndex(rowId),
                Bytes.toBytes(JacobiEigenValue.EICOL + pivotrow)).getValue());
            s2 = BytesUtil.bytesToDouble(htable.get(
                BytesUtil.getRowIndex(rowId),
                Bytes.toBytes(JacobiEigenValue.EICOL + pivotcol)).getValue());

            bu = new VectorUpdate(rowId);
            bu.put(EICOL, pivotrow, pivotcos * s1 - pivotsin * s2);
            bu.put(EICOL, pivotcol, pivotsin * s1 + pivotcos * s2);

            htable.commit(bu.getBatchUpdate());
          } else if (rowId == pivotrow) {
            return true;
          } else if (rowId < pivotcol) {
            s1 = BytesUtil.bytesToDouble(htable.get(
                BytesUtil.getRowIndex(pivotrow), Bytes.toBytes(EICOL + rowId))
                .getValue());
            s2 = BytesUtil.bytesToDouble(htable.get(
                BytesUtil.getRowIndex(rowId), Bytes.toBytes(EICOL + pivotcol))
                .getValue());

            bu = new VectorUpdate(rowId);
            bu.put(EICOL, pivotcol, pivotsin * s1 + pivotcos * s2);
            htable.commit(bu.getBatchUpdate());

            bu = new VectorUpdate(pivotrow);
            bu.put(EICOL, rowId, pivotcos * s1 - pivotsin * s2);
            htable.commit(bu.getBatchUpdate());
          } else if (rowId == pivotcol) {
            for (int i = pivotcol + 1; i < size; i++) {
              s1 = BytesUtil.bytesToDouble(htable.get(
                  BytesUtil.getRowIndex(pivotrow), Bytes.toBytes(EICOL + i))
                  .getValue());
              s2 = BytesUtil.bytesToDouble(htable.get(
                  BytesUtil.getRowIndex(pivotcol), Bytes.toBytes(EICOL + i))
                  .getValue());

              bu = new VectorUpdate(pivotcol);
              bu.put(EICOL, i, pivotsin * s1 + pivotcos * s2);
              htable.commit(bu.getBatchUpdate());

              bu = new VectorUpdate(pivotrow);
              bu.put(EICOL, i, pivotcos * s1 - pivotsin * s2);
              htable.commit(bu.getBatchUpdate());
            }
          } else { // rowId > pivotcol
            return false;
          }

          lastRow = row;
          processedRows++;
        }
        return hasMore;
      }

      @Override
      public float getProgress() {
        if (totalRows <= 0) {
          return 0;
        } else {
          return Math.min(1.0f, processedRows / (float) totalRows);
        }
      }

    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      InputSplit[] splits = super.getSplits(job, numSplits);
      List<InputSplit> newSplits = new ArrayList<InputSplit>();
      for (InputSplit split : splits) {
        TableSplit ts = (TableSplit) split;
        byte[] row = ts.getStartRow();
        if (row.length == 0) // the first split
          newSplits.add(split);
        else {
          if (BytesUtil.bytesToInt(ts.getStartRow()) < pivot_col) {
            newSplits.add(split);
          }
        }
      }

      return newSplits.toArray(new InputSplit[newSplits.size()]);
    }

    @Override
    public RecordReader<NullWritable, NullWritable> getRecordReader(
        InputSplit split, JobConf conf, Reporter reporter) throws IOException {
      TableSplit tSplit = (TableSplit) split;
      RotationRecordReader trr = this.tableRecordReader;
      // if no table record reader was provided use default
      if (trr == null) {
        trr = new RotationRecordReader(pivot_row, pivot_col, pivot_sin,
            pivot_cos);
      }
      trr.setStartRow(tSplit.getStartRow());
      trr.setEndRow(tSplit.getEndRow());
      trr.setHTable(this.table);
      trr.setInputColumns(this.inputColumns);
      trr.setRowFilter(this.rowFilter);
      trr.init();
      return trr;
    }

    protected void setTableRecordReader(RotationRecordReader tableRecordReader) {
      this.tableRecordReader = tableRecordReader;
    }

  }
}
