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
package org.apache.hama.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.Vertex;

public class CustomVertexReadWriteStateTest extends TestCase {
  static int initialState = 2;
  static int changedState = 4;

  public static class TestVertex extends Vertex<Text, IntWritable, IntWritable> {

    private static ArrayWritable test = new ArrayWritable(IntWritable.class);

    @Override
    public void setup(HamaConfiguration conf) {
      // Sets the initial state
      test.set(new Writable[] { new IntWritable(initialState) });
    }

    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {
      if (this.getSuperstepCount() == 3) {
        // change the state
        test.set(new Writable[] { new IntWritable(changedState) });
      }

      if (this.getSuperstepCount() < 3) {
        assertEquals(initialState, ((IntWritable) test.get()[0]).get());
      } else {
        assertEquals(changedState, ((IntWritable) test.get()[0]).get());
      }
    }

    public void readState(DataInput in) throws IOException {
      test.readFields(in);
    }

    public void writeState(DataOutput out) throws IOException {
      test.write(out);
    }
  }

}
