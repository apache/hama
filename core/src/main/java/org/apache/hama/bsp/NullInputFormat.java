package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;

public class NullInputFormat implements InputFormat<NullWritable, NullWritable> {

  @Override
  public RecordReader<NullWritable, NullWritable> getRecordReader(
      InputSplit split, BSPJob job) throws IOException {
    return new NullRecordReader();
  }

  @Override
  public InputSplit[] getSplits(BSPJob job, int numBspTask) throws IOException {
    InputSplit[] splits = new InputSplit[numBspTask];
    for (int i = 0; i < numBspTask; i++) {
      splits[i] = new NullInputSplit();
    }

    return splits;
  }

  public static class NullRecordReader implements
      RecordReader<NullWritable, NullWritable> {
    private boolean returnRecord = true;

    @Override
    public void close() throws IOException {
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public NullWritable createValue() {
      return NullWritable.get();
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }

    @Override
    public boolean next(NullWritable key, NullWritable value)
        throws IOException {
      if (returnRecord == true) {
        returnRecord = false;
        return true;
      }

      return returnRecord;
    }

  }

  public static class NullInputSplit implements InputSplit {
    public long getLength() {
      return 0;
    }

    public String[] getLocations() {
      String[] locs = {};
      return locs;
    }

    public void readFields(DataInput in) throws IOException {
    }

    public void write(DataOutput out) throws IOException {
    }
  }

}
