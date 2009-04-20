package org.apache.hama.algebra;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseVector;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.VectorOutputFormat;
import org.apache.log4j.Logger;

public class DenseMatrixVectorMultReduce extends MapReduceBase implements
    Reducer<IntWritable, MapWritable, IntWritable, VectorUpdate> {
  static final Logger LOG = Logger.getLogger(DenseMatrixVectorMultReduce.class);
  
  /**
   * Use this before submitting a TableReduce job. It will appropriately set up
   * the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initJob(String table,
      Class<DenseMatrixVectorMultReduce> reducer, JobConf job) {
    job.setOutputFormat(VectorOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(VectorOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BatchUpdate.class);
  }
  
  @Override
  public void reduce(IntWritable key, Iterator<MapWritable> values,
      OutputCollector<IntWritable, VectorUpdate> output, Reporter reporter)
      throws IOException {
    DenseVector sum = new DenseVector();
    
    while (values.hasNext()) {
      DenseVector nVector = new DenseVector(values.next());
      if(sum.size() == 0) {
        sum.zeroFill(nVector.size());
        sum.add(nVector);
      } else {
        sum.add(nVector);
      }
    }

    VectorUpdate update = new VectorUpdate(key.get());
    update.putAll(sum.getEntries());

    output.collect(key, update);
  }

}
