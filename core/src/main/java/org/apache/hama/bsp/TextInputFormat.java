package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TextInputFormat extends FileInputFormat<LongWritable, Text> {

  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, BSPJob job)
      throws IOException {
    return new LineRecordReader(job.getConf(), (FileSplit) split);
  }

}
