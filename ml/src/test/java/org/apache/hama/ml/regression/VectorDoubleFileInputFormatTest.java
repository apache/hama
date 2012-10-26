/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.ml.regression;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.FileSplit;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.writable.VectorWritable;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Testcase for {@link VectorDoubleFileInputFormat}
 */
public class VectorDoubleFileInputFormatTest {

  @Test
  public void testFileRead() throws Exception {
    VectorDoubleFileInputFormat inputFormat = new VectorDoubleFileInputFormat();
    Path file = new Path("src/test/resources/vd_file_sample.txt");
    InputSplit split = new FileSplit(file, 0, 1000, new String[]{"localhost"});
    BSPJob job = new BSPJob();
    RecordReader<VectorWritable, DoubleWritable> recordReader = inputFormat.getRecordReader(split, job);
    assertNotNull(recordReader);
    VectorWritable key = recordReader.createKey();
    assertNotNull(key);
    DoubleWritable value = recordReader.createValue();
    assertNotNull(value);
    assertTrue(recordReader.next(key, value));
    assertEquals(new DenseDoubleVector(new double[]{2d, 3d, 4d}), key.getVector());
    assertEquals(new DoubleWritable(1d), value);
  }

}
