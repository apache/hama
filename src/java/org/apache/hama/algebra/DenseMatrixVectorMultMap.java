package org.apache.hama.algebra;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.log4j.Logger;

public class DenseMatrixVectorMultMap extends MapReduceBase implements
    Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
  static final Logger LOG = Logger.getLogger(DenseMatrixVectorMultMap.class);
  protected DenseVector currVector;
  public static final String ITH_ROW = "ith.row";
  public static final String MATRIX_A = "hama.multiplication.matrix.a";
  public static final String MATRIX_B = "hama.multiplication.matrix.b";
  private IntWritable nKey = new IntWritable();
  
  public void configure(JobConf job) {
    DenseMatrix matrix_a;
      try {
        matrix_a = new DenseMatrix(new HamaConfiguration(job), job.get(MATRIX_A, ""));
        int ithRow = job.getInt(ITH_ROW, 0);
        nKey.set(ithRow);
        currVector = matrix_a.getRow(ithRow);
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  public static void initJob(int i, String matrix_a, String matrix_b,
      Class<DenseMatrixVectorMultMap> map, Class<IntWritable> outputKeyClass,
      Class<MapWritable> outputValueClass, JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.setInt(ITH_ROW, i);
    jobConf.set(MATRIX_A, matrix_a);
    jobConf.set(MATRIX_B, matrix_b);
    
    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, matrix_b);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, MapWritable value,
      OutputCollector<IntWritable, MapWritable> output, Reporter reporter)
      throws IOException {

    DenseVector scaled = new DenseVector(value).scale(currVector.get(key.get()));
    output.collect(nKey, scaled.getEntries());
    
  }
}