package org.apache.hama.examples;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.matrix.Vector;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

public class TestCosineSimilarityMatrix extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestCosineSimilarityMatrix.class);
  private int SIZE = 10;
  private Matrix m1;
  private Matrix symmetricMatrix;
  private HamaConfiguration conf;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestCosineSimilarityMatrix() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();

    m1 = DenseMatrix.random(conf, SIZE, SIZE);
    symmetricMatrix = new DenseMatrix(conf, SIZE, SIZE);
  }

  public void testCosineSimilarity() throws IOException {
    Job job = new Job(conf, "set MR job test");
    job.getConfiguration().set("input.matrix", m1.getPath());

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(m1.getPath(), scan,
        ComputeSimilarity.class, ImmutableBytesWritable.class, Put.class, job);
    TableMapReduceUtil.initTableReducerJob(symmetricMatrix.getPath(),
        IdentityTableReducer.class, job);
    job.setNumReduceTasks(0);
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    Vector v1 = m1.getRow(0);
    Vector v2 = m1.getRow(2);
    assertEquals(v1.dot(v2), symmetricMatrix.get(0, 2));
  }

  public static class ComputeSimilarity extends
      TableMapper<ImmutableBytesWritable, Put> implements Configurable {
    private Configuration conf = null;
    private DenseMatrix matrix;

    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      DenseVector v = new DenseVector(value);

      Put put = new Put(key.get());
      for (int i = 0; i < matrix.getRows(); i++) {
        double dotProduct = matrix.getRow(i).dot(v);
        if (BytesUtil.getRowIndex(key.get()) == i) {
          dotProduct = 0;
        }
        put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String
            .valueOf(i)), Bytes.toBytes(dotProduct));
      }

      context.write(key, put);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      try {
        matrix = new DenseMatrix(new HamaConfiguration(conf), conf
            .get("input.matrix"));
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }
}
