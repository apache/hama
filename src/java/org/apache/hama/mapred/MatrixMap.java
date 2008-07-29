package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorDatum;

@SuppressWarnings("unchecked")
public abstract class MatrixMap<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements Mapper<ImmutableBytesWritable, VectorDatum, K, V> {
  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   * 
   * @param table table name
   * @param columns columns to scan
   * @param mapper mapper class
   * @param job job configuration
   */
  public static void initJob(String table, String columns,
    Class<? extends MatrixMap> mapper, 
    Class<? extends WritableComparable> outputKeyClass, 
    Class<? extends Writable> outputValueClass, JobConf job) {
      
    job.setInputFormat(MatrixInputFormat.class);
    job.setMapOutputValueClass(outputValueClass);
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    FileInputFormat.addInputPaths(job, table);
    job.set(TableInputFormat.COLUMN_LIST, columns);
  }

  /**
   * Call a user defined function on a single HBase record, represented
   * by a key and its associated record value.
   * 
   * @param key
   * @param value
   * @param output
   * @param reporter
   * @throws IOException
   */
  public abstract void map(ImmutableBytesWritable key, VectorDatum value,
      OutputCollector<K, V> output, Reporter reporter) throws IOException;
}
