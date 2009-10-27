package org.apache.hama.matrix.algebra;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

public class MatrixAdditionMap extends TableMapper<IntWritable, MapWritable>
    implements Configurable {
  static final Logger LOG = Logger.getLogger(MatrixAdditionMap.class);
  private Configuration conf = null;
  protected DenseMatrix[] matrix_summands;
  protected double[] matrix_alphas;
  public static final String MATRIX_SUMMANDS = "hama.addition.summands";
  public static final String MATRIX_ALPHAS = "hama.addition.alphas";

  public void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    IntWritable nKey = new IntWritable(BytesUtil.getRowIndex(key.get()));

    DenseVector result = new DenseVector(value);
    DenseVector summand;
    for (int i = 0; i < matrix_summands.length; i++) {
      summand = matrix_summands[i].getRow(nKey.get());
      result = result.add(matrix_alphas[i], summand);
    }
    context.write(nKey, result.getEntries());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      String[] matrix_names = conf.get(MATRIX_SUMMANDS, "").split(",");
      String[] matrix_alpha_strs = conf.get(MATRIX_ALPHAS, "").split(",");
      assert (matrix_names.length == matrix_alpha_strs.length && matrix_names.length >= 1);

      matrix_summands = new DenseMatrix[matrix_names.length];
      matrix_alphas = new double[matrix_names.length];
      for (int i = 0; i < matrix_names.length; i++) {
        matrix_summands[i] = new DenseMatrix(new HamaConfiguration(conf),
            matrix_names[i]);
        matrix_alphas[i] = Double.valueOf(matrix_alpha_strs[i]);
      }
    } catch (IOException e) {
      LOG.warn("Load matrix_b failed : " + e.getMessage());
    }
  }

}
