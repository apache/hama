package org.apache.hama.matrix.algebra;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hama.Constants;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

/** A Catalog class collect all the mr classes to compute the matrix's norm */
public class MatrixNormMapReduce {
  public final static IntWritable nKey = new IntWritable(-1);

  /** Infinity Norm */
  public static class MatrixInfinityNormMapper extends
      TableMapper<IntWritable, DoubleWritable> {

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {

      double rowSum = 0;
      NavigableMap<byte[], byte[]> v = value
          .getFamilyMap(Constants.COLUMNFAMILY);
      for (Map.Entry<byte[], byte[]> e : v.entrySet()) {
        rowSum += Math.abs(BytesUtil.bytesToDouble(e.getValue()));
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(rowSum));
    }
  }

  /**
   * Matrix Infinity Norm Reducer
   */
  public static class MatrixInfinityNormReduce extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    static final Logger LOG = Logger.getLogger(MatrixInfinityNormReduce.class);
    private double max = 0;

    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      for (DoubleWritable val : values) {
        max = Math.max(val.get(), max);
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(max));
    }
  }

  /** One Norm Mapper */
  public static class MatrixOneNormMapper extends
      TableMapper<IntWritable, DoubleWritable> {

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {

      NavigableMap<byte[], byte[]> v = value
          .getFamilyMap(Constants.COLUMNFAMILY);
      for (Map.Entry<byte[], byte[]> e : v.entrySet()) {
        context.write(new IntWritable(BytesUtil.bytesToInt(e.getKey())),
            new DoubleWritable(BytesUtil.bytesToDouble(e.getValue())));
      }
    }
  }

  /** One Norm Combiner * */
  public static class MatrixOneNormCombiner extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {

      double partialColSum = 0;
      for (DoubleWritable val : values) {
        partialColSum += val.get();
      }

      context.write(key, new DoubleWritable(partialColSum));
    }
  }

  /** One Norm Reducer * */
  public static class MatrixOneNormReducer extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private double max = 0;

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      double colSum = 0;

      for (DoubleWritable val : values) {
        colSum += val.get();
      }

      max = Math.max(Math.abs(colSum), max);
    }

    public void cleanup(Context context) throws IOException,
        InterruptedException {
      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(max));
    }
  }

  /** Frobenius Norm Mapper */
  public static class MatrixFrobeniusNormMapper extends
      TableMapper<IntWritable, DoubleWritable> {

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      double rowSqrtSum = 0;

      NavigableMap<byte[], byte[]> v = value
          .getFamilyMap(Constants.COLUMNFAMILY);
      for (Map.Entry<byte[], byte[]> e : v.entrySet()) {
        double cellValue = BytesUtil.bytesToDouble(e.getValue());
        rowSqrtSum += (cellValue * cellValue);
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(rowSqrtSum));
    }
  }

  /** Frobenius Norm Combiner */
  public static class MatrixFrobeniusNormCombiner extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private double sqrtSum = 0;

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      for (DoubleWritable val : values) {
        sqrtSum += val.get();
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(sqrtSum));
    }
  }

  /** Frobenius Norm Reducer */
  public static class MatrixFrobeniusNormReducer extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private double sqrtSum = 0;

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      for (DoubleWritable val : values) {
        sqrtSum += val.get();
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(Math
          .sqrt(sqrtSum)));
    }
  }

  /** MaxValue Norm Mapper * */
  public static class MatrixMaxValueNormMapper extends
      TableMapper<IntWritable, DoubleWritable> {

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      double max = 0;

      NavigableMap<byte[], byte[]> v = value
          .getFamilyMap(Constants.COLUMNFAMILY);
      for (Map.Entry<byte[], byte[]> e : v.entrySet()) {
        double cellValue = BytesUtil.bytesToDouble(e.getValue());
        max = cellValue > max ? cellValue : max;
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(max));
    }
  }

  /** MaxValue Norm Reducer */
  public static class MatrixMaxValueNormReducer extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private double max = 0;

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
        Context context) throws IOException, InterruptedException {
      for (DoubleWritable val : values) {
        max = Math.max(val.get(), max);
      }

      context.write(MatrixNormMapReduce.nKey, new DoubleWritable(max));
    }
  }
}
