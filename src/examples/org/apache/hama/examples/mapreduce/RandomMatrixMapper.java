package org.apache.hama.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.matrix.SparseVector;
import org.apache.hama.matrix.Vector;
import org.apache.hama.util.RandomVariable;
import org.apache.log4j.Logger;

public class RandomMatrixMapper extends
    Mapper<IntWritable, IntWritable, IntWritable, MapWritable> implements
    Configurable {
  private Configuration conf = null;
  static final Logger LOG = Logger.getLogger(RandomMatrixMapper.class);
  protected int column;
  protected double density;
  protected int minNums;
  protected String type;
  protected Vector vector = new DenseVector();

  public void map(IntWritable key, IntWritable value,
      Context context)
      throws IOException, InterruptedException {

    if (type.equals("SparseMatrix")) {
      for (int i = key.get(); i <= value.get(); i++) {
        ((SparseVector) vector).clear();
        for (int j = 0; j < minNums; j++) {
          ((SparseVector) vector).set(RandomVariable.randInt(0, column - 1),
              RandomVariable.rand());
        }
        context.write(new IntWritable(i), vector.getEntries());
      }
    } else {
      for (int i = key.get(); i <= value.get(); i++) {
        ((DenseVector) vector).clear();
        for (int j = 0; j < column; j++) {
          ((DenseVector) vector).set(j, RandomVariable.rand());
        }
        context.write(new IntWritable(i), vector.getEntries());
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
    column = conf.getInt("matrix.column", 0);
    density = Double.parseDouble(conf.get("matrix.density"));

    double vv = (column / 100.0) * density;
    minNums = Math.round((float) vv);
    if (minNums == 0)
      minNums = 1;

    type = conf.get("matrix.type");
    if (type.equals("SparseMatrix"))
      vector = new SparseVector();
    else
      vector = new DenseVector();
  }
}
