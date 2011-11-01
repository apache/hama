package org.apache.hama.bsp;

import java.io.IOException;

public interface InputFormat<K, V> {

  InputSplit[] getSplits(BSPJob job, int numBspTask) throws IOException;

  RecordReader<K, V> getRecordReader(InputSplit split, BSPJob job) throws IOException;

}
