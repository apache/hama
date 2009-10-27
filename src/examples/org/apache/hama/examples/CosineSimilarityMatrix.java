package org.apache.hama.examples;

import java.io.IOException;

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
import org.apache.hama.HamaAdmin;
import org.apache.hama.HamaAdminImpl;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.util.BytesUtil;

/**
 * Cosine Similarity MapReduce
 * 
 * <p>
 * This is EXAMPLE code. You will need to change it to work for your context.
 * <p>
 * 
 * <pre>
 * ./bin/hama examples similarity INPUT_MATRIX OUTPUT_NAME
 * </pre>
 */
public class CosineSimilarityMatrix {

  public static class ComputeSimilarityMapper extends
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
            .valueOf(i)), BytesUtil.doubleToBytes(dotProduct));
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

  private static Job configureJob(HamaConfiguration conf, String[] args)
      throws IOException {
    HamaAdmin admin = new HamaAdminImpl(conf);
    
    Job job = new Job(conf, "set MR job test");
    job.getConfiguration().set("input.matrix", admin.getPath(args[0]));

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(admin.getPath(args[0]), scan,
        ComputeSimilarityMapper.class, ImmutableBytesWritable.class, Put.class, job);
    TableMapReduceUtil.initTableReducerJob(args[1], IdentityTableReducer.class,
        job);
    job.setNumReduceTasks(0);
    return job;
  }

  /**
   * <input matrix> <output similarity matrix>
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage:  <input matrix> <output similarity matrix>");
    }

    HamaConfiguration conf = new HamaConfiguration();
    Job job = configureJob(conf, args);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
