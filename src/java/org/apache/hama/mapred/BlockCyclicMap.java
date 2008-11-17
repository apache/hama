package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.Matrix;
import org.apache.hama.io.BlockWritable;
import org.apache.log4j.Logger;

public abstract class BlockCyclicMap<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements Mapper<IntWritable, BlockWritable, K, V> {
  static final Logger LOG = Logger.getLogger(BlockCyclicMap.class);
  public static Matrix MATRIX_B;

  public static void initJob(String matrixA,
      Class<? extends BlockCyclicMap> mapper, JobConf job) {

    job.setInputFormat(BlockInputFormat.class);
    job.setMapperClass(mapper);
    FileInputFormat.addInputPaths(job, matrixA);

    job.set(BlockInputFormat.COLUMN_LIST, Constants.BLOCK);
  }

  public abstract void map(IntWritable key, BlockWritable value,
      OutputCollector<K, V> output, Reporter reporter) throws IOException;
}
