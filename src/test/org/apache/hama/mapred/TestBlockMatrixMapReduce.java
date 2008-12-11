package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.DenseMatrix;
import org.apache.hama.HCluster;
import org.apache.hama.Matrix;
import org.apache.hama.algebra.BlockCyclicMultiplyMap;
import org.apache.hama.algebra.BlockCyclicMultiplyReduce;
import org.apache.hama.io.BlockWritable;
import org.apache.log4j.Logger;

public class TestBlockMatrixMapReduce extends HCluster {
  static final Logger LOG = Logger.getLogger(TestBlockMatrixMapReduce.class);
  static Matrix c;
  static final int SIZE = 20;
  /** constructor */
  public TestBlockMatrixMapReduce() {
    super();
  }

  public void testBlockMatrixMapReduce() throws IOException, ClassNotFoundException {
    Matrix m1 = DenseMatrix.random(conf, SIZE, SIZE);
    Matrix m2 = DenseMatrix.random(conf, SIZE, SIZE);
    ((DenseMatrix) m1).blocking_mapred(4);
    ((DenseMatrix) m2).blocking_mapred(4);

    miniMRJob(m1.getPath(), m2.getPath());
    
    double[][] C = new double[SIZE][SIZE];
    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        for (int k = 0; k < SIZE; k++) {
          C[i][k] += m1.get(i, j) * m2.get(j, k);
        }
      }
    }

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertEquals(String.valueOf(C[i][j]).substring(0, 5), 
            String.valueOf(c.get(i, j)).substring(0, 5));
      }
    }
  }

  private void miniMRJob(String string, String string2) throws IOException {
    c = new DenseMatrix(conf);
    String output = c.getPath();
    
    JobConf jobConf = new JobConf(conf, TestBlockMatrixMapReduce.class);
    jobConf.setJobName("test MR job");

    BlockCyclicMultiplyMap.initJob(string, string2, BlockCyclicMultiplyMap.class, IntWritable.class,
        BlockWritable.class, jobConf);
    BlockCyclicReduce.initJob(output, BlockCyclicMultiplyReduce.class, jobConf);

    jobConf.setNumMapTasks(2);
    jobConf.setNumReduceTasks(2);

    JobClient.runJob(jobConf);
  }
}
