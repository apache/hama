package org.apache.hama.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("unchecked")
public abstract class MatrixReduce<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements Reducer<K, V, ImmutableBytesWritable, BatchUpdate> {
  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initJob(String table,
      Class<? extends MatrixReduce> reducer, JobConf job) {
    job.setOutputFormat(TableOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
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
    OutputCollector<ImmutableBytesWritable, BatchUpdate> output, Reporter reporter)
  throws IOException;
}