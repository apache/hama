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

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

public class ClassSerializePrinting extends
    BSP<NullWritable, NullWritable, IntWritable, Text, MapWritable> {

  public static final int NUM_SUPERSTEPS = 15;

  @Override
  public void bsp(
      BSPPeer<NullWritable, NullWritable, IntWritable, Text, MapWritable> bspPeer)
      throws IOException, SyncException, InterruptedException {

    for (int i = 0; i < NUM_SUPERSTEPS; i++) {
      for (String otherPeer : bspPeer.getAllPeerNames()) {
        MapWritable map = new MapWritable();
        map.put(new Text(bspPeer.getPeerName()), new IntWritable(i));

        bspPeer.send(otherPeer, map);
      }
      bspPeer.sync();

      MapWritable msg = null;
      while ((msg = bspPeer.getCurrentMessage()) != null) {
        for (Entry<Writable, Writable> e : msg.entrySet()) {
          bspPeer.write((IntWritable) e.getValue(), (Text) e.getKey());
        }
      }
    }
  }
}
