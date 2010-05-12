package org.apache.hama.examples;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hama.Constants;
import org.apache.hama.HamaAdmin;
import org.apache.hama.HamaAdminImpl;
import org.apache.hama.examples.mapreduce.DummyMapper;
import org.apache.hama.examples.mapreduce.JacobiInitMap;
import org.apache.hama.examples.mapreduce.PivotInputFormat;
import org.apache.hama.examples.mapreduce.PivotMap;
import org.apache.hama.examples.mapreduce.RotationInputFormat;
import org.apache.hama.io.Pair;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.util.BytesUtil;

public class JacobiEigen extends AbstractExample {

  /**
   * EigenValue Constants
   */
  /** a matrix copy of the original copy collected in "eicol" family * */
  public static final String EICOL = "eicol";

  /** a column family collect all values and statuses used during computation * */
  public static final String EI = "eival";
  public static final String EIVAL = "value";
  public static final String EICHANGED = "changed";

  /** a column identify the index of the max absolute value each row * */
  public static final String EIIND = "ind";

  /** a matrix collect all the eigen vectors * */
  public static final String EIVEC = "eivec";
  public static final String MATRIX = "hama.jacobieigenvalue.matrix";

  /** parameters for pivot * */
  public static final String PIVOTROW = "hama.jacobi.pivot.row";
  public static final String PIVOTCOL = "hama.jacobi.pivot.col";
  public static final String PIVOTSIN = "hama.jacobi.pivot.sin";
  public static final String PIVOTCOS = "hama.jacobi.pivot.cos";
  
  private static HTable table;

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out
          .println("add [-m maps] [-r reduces] <matrix A> [max_iteration]");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    String matrixA = ARGS.get(0);

    HamaAdmin admin = new HamaAdminImpl(conf);
    Matrix a = admin.getMatrix(matrixA);

    if (ARGS.size() > 1) {
      jacobiEigenValue((DenseMatrix) a, Integer.parseInt(ARGS.get(1)));
    } else {
      jacobiEigenValue((DenseMatrix) a, Integer.MAX_VALUE);
    }
  }

  /**
   * Compute all the eigen values. Note: all the eigen values are collected in
   * the "eival:value" column, and the eigen vector of a specified eigen value
   * is collected in the "eivec:" column family in the same row.
   * 
   * TODO: we may need to expose the interface to access the eigen values and
   * vectors
   * 
   * @param imax limit the loops of the computation
   * @throws IOException
   */
  public static void jacobiEigenValue(DenseMatrix m, int imax)
      throws IOException {
    table = new HTable(conf, m.getPath());
    /*
     * Initialization A M/R job is used for initialization(such as, preparing a
     * matrx copy of the original in "eicol:" family.)
     */
    // initialization
    Job job = new Job(conf, "JacobiEigen initialization MR job" + m.getPath());

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(m.getPath(), scan,
        JacobiInitMap.class, ImmutableBytesWritable.class, Put.class, job);
    TableMapReduceUtil.initTableReducerJob(m.getPath(),
        IdentityTableReducer.class, job);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    final FileSystem fs = FileSystem.get(conf);
    Pair pivotPair = new Pair();
    DoubleWritable pivotWritable = new DoubleWritable();
    Put put;

    // loop
    int size = m.getRows();
    int state = size;
    int pivot_row, pivot_col;
    double pivot;
    double s, c, t, y;

    int icount = 0;
    while (state != 0 && icount < imax) {
      icount = icount + 1;
      /*
       * Find the pivot and its index(pivot_row, pivot_col) A M/R job is used to
       * scan all the "eival:ind" to get the max absolute value of each row, and
       * do a MAX aggregation of these max values to get the max value in the
       * matrix.
       */
      Path outDir = new Path(new Path(m.getType() + "_TMP_FindPivot_dir_"
          + System.currentTimeMillis()), "out");
      if (fs.exists(outDir))
        fs.delete(outDir, true);

      job = new Job(conf, "Find Pivot MR job" + m.getPath());

      scan = new Scan();
      scan.addFamily(Bytes.toBytes(EI));

      job.setInputFormatClass(PivotInputFormat.class);
      job.setMapOutputKeyClass(Pair.class);
      job.setMapOutputValueClass(DoubleWritable.class);
      job.setMapperClass(PivotMap.class);
      job.getConfiguration().set(PivotInputFormat.INPUT_TABLE, m.getPath());
      job.getConfiguration().set(PivotInputFormat.SCAN,
          PivotInputFormat.convertScanToString(scan));

      job.setOutputKeyClass(Pair.class);
      job.setOutputValueClass(DoubleWritable.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(job, outDir);

      try {
        job.waitForCompletion(true);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }

      // read outputs
      Path inFile = new Path(outDir, "part-r-00000");
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);
      try {
        reader.next(pivotPair, pivotWritable);
        pivot_row = pivotPair.getRow();
        pivot_col = pivotPair.getColumn();
        pivot = pivotWritable.get();
      } finally {
        reader.close();
      }
      fs.delete(outDir, true);
      fs.delete(outDir.getParent(), true);

      if (pivot_row == 0 && pivot_col == 0)
        break; // stop the iterations

      /*
       * Calculation Compute the rotation parameters of next rotation.
       */
      Get get = new Get(BytesUtil.getRowIndex(pivot_row));
      get.addFamily(Bytes.toBytes(EI));
      Result r = table.get(get);
      double e1 = Bytes.toDouble(r.getValue(Bytes.toBytes(EI), Bytes
          .toBytes(EIVAL)));

      get = new Get(BytesUtil.getRowIndex(pivot_col));
      get.addFamily(Bytes.toBytes(EI));
      r = table.get(get);
      double e2 = Bytes.toDouble(r.getValue(Bytes.toBytes(EI), Bytes
          .toBytes(EIVAL)));

      y = (e2 - e1) / 2;
      t = Math.abs(y) + Math.sqrt(pivot * pivot + y * y);
      s = Math.sqrt(pivot * pivot + t * t);
      c = t / s;
      s = pivot / s;
      t = (pivot * pivot) / t;
      if (y < 0) {
        s = -s;
        t = -t;
      }

      /*
       * Upate the pivot and the eigen values indexed by the pivot
       */
      put = new Put(BytesUtil.getRowIndex(pivot_row));
      put.add(Bytes.toBytes(EICOL), Bytes.toBytes(String
          .valueOf(pivot_col)), Bytes.toBytes(0.0));
      table.put(put);

      state = update(pivot_row, -t, state);
      state = update(pivot_col, t, state);

      /*
       * Rotation the matrix
       */
      job = new Job(conf, "Rotation Matrix MR job" + m.getPath());

      scan = new Scan();
      scan.addFamily(Bytes.toBytes(EI));

      job.getConfiguration().setInt(PIVOTROW, pivot_row);
      job.getConfiguration().setInt(PIVOTCOL, pivot_col);
      job.getConfiguration().set(PIVOTSIN, String.valueOf(s));
      job.getConfiguration().set(PIVOTCOS, String.valueOf(c));

      job.setInputFormatClass(RotationInputFormat.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setMapperClass(DummyMapper.class);
      job.getConfiguration().set(RotationInputFormat.INPUT_TABLE, m.getPath());
      job.getConfiguration().set(RotationInputFormat.SCAN,
          PivotInputFormat.convertScanToString(scan));
      job.setOutputFormatClass(NullOutputFormat.class);

      try {
        job.waitForCompletion(true);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }

      // rotate eigenvectors
      for (int i = 0; i < size; i++) {
        get = new Get(BytesUtil.getRowIndex(pivot_row));
        e1 = Bytes.toDouble(table.get(get).getValue(
            Bytes.toBytes(EIVEC), Bytes.toBytes(String.valueOf(i))));

        get = new Get(BytesUtil.getRowIndex(pivot_col));
        e2 = Bytes.toDouble(table.get(get).getValue(
            Bytes.toBytes(EIVEC), Bytes.toBytes(String.valueOf(i))));

        double pivotR = (c * e1 - s * e2);
        put = new Put(BytesUtil.getRowIndex(pivot_row));
        put.add(Bytes.toBytes(EIVEC), Bytes
            .toBytes(String.valueOf(i)), Bytes.toBytes(pivotR));
        table.put(put);

        double eivC = (s * e1 + c  * e2);
        put = new Put(BytesUtil.getRowIndex(pivot_col));
        put.add(Bytes.toBytes(EIVEC), Bytes
            .toBytes(String.valueOf(i)), Bytes.toBytes(eivC));
        table.put(put);
      }

      // update index array
      maxind(pivot_row, size);
      maxind(pivot_col, size);
    }
  }

  static void maxind(int row, int size) throws IOException {
    int m = row + 1;
    Get get = null;
    if (row + 2 < size) {
      get = new Get(BytesUtil.getRowIndex(row));

      double max = Bytes.toDouble(table.get(get).getValue(
          Bytes.toBytes(EICOL), Bytes.toBytes(String.valueOf(m))));
      double val;
      for (int i = row + 2; i < size; i++) {
        get = new Get(BytesUtil.getRowIndex(row));
        val = Bytes.toDouble(table.get(get).getValue(
            Bytes.toBytes(EICOL), Bytes.toBytes(String.valueOf(i))));
        if (Math.abs(val) > Math.abs(max)) {
          m = i;
          max = val;
        }
      }
    }

    Put put = new Put(BytesUtil.getRowIndex(row));
    put.add(Bytes.toBytes(EI), Bytes.toBytes("ind"), Bytes
        .toBytes(String.valueOf(m)));
    table.put(put);
  }

  static int update(int row, double value, int state) throws IOException {
    Get get = new Get(BytesUtil.getRowIndex(row));
    double e = Bytes.toDouble(table.get(get).getValue(
        Bytes.toBytes(EI), Bytes.toBytes(EIVAL)));
    int changed = BytesUtil.bytesToInt(table.get(get).getValue(
        Bytes.toBytes(EI), Bytes.toBytes("changed")));
    double y = e;
    e += value;

    Put put = new Put(BytesUtil.getRowIndex(row));
    put.add(Bytes.toBytes(EI), Bytes.toBytes(EIVAL), Bytes
        .toBytes(e));

    if (changed == 1 && (Math.abs(y - e) < .0000001)) { // y == e) {
      changed = 0;
      put.add(Bytes.toBytes(EI), Bytes.toBytes(EICHANGED),
          Bytes.toBytes(String.valueOf(changed)));

      state--;
    } else if (changed == 0 && (Math.abs(y - e) > .0000001)) {
      changed = 1;
      put.add(Bytes.toBytes(EI), Bytes.toBytes(EICHANGED),
          Bytes.toBytes(String.valueOf(changed)));

      state++;
    }
    table.put(put);
    return state;
  }
  
  // for test
  static boolean verifyEigenValue(double[] e, double[][] E) throws IOException {
    boolean success = true;
    double e1, ev;
    Get get = null;
    for (int i = 0; i < e.length; i++) {
      get = new Get(BytesUtil.getRowIndex(i));
      e1 = Bytes.toDouble(table.get(get).getValue(Bytes.toBytes(EI),
          Bytes.toBytes(EIVAL)));
      success &= ((Math.abs(e1 - e[i]) < .0000001));
      if (!success)
        return success;

      for (int j = 0; j < E[i].length; j++) {
        get = new Get(BytesUtil.getRowIndex(i));
        ev = Bytes.toDouble(table.get(get).getValue(
            Bytes.toBytes(EIVEC), Bytes.toBytes(String.valueOf(j))));
        success &= ((Math.abs(ev - E[i][j]) < .0000001));
        if (!success)
          return success;
      }
    }
    return success;
  }
}
