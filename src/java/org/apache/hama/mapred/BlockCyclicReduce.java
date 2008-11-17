package org.apache.hama.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorUpdate;

public abstract class BlockCyclicReduce<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements Reducer<K, V, IntWritable, VectorUpdate> {
  /**
   * Use this before submitting a TableReduce job. It will appropriately set up
   * the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initJob(String table,
      Class<? extends BlockCyclicReduce> reducer, JobConf job) {
    job.setOutputFormat(VectorOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(VectorOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BatchUpdate.class);
  }

  /**
   * 
   * @param key
   * @param values
   * @param output
   * @param reporter
   * @throws IOException
   */
  public abstract void reduce(K key, Iterator<V> values,
      OutputCollector<IntWritable, VectorUpdate> output, Reporter reporter)
      throws IOException;
}
