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
package org.apache.hama.pipes;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hama.bsp.Partitioner;

/**
 * 
 * PipesPartitioner is a Wrapper for C++ Partitioner Java Partitioner ->
 * BinaryProtocol -> C++ Partitioner and back
 * 
 */
public class PipesPartitioner<K, V> implements Partitioner<K, V> {

  private static final Log LOG = LogFactory.getLog(PipesPartitioner.class);
  private PipesApplication<K, V, ?, ?, BytesWritable> application = new PipesApplication<K, V, Object, Object, BytesWritable>();

  public PipesPartitioner(Configuration conf) {
    LOG.debug("Start Pipes client for PipesPartitioner.");
    try {
      application.start(conf);
    } catch (IOException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
  }

  public void cleanup() {
    try {
      application.cleanup(true);
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  /**
   * Partitions a specific key value mapping to a bucket.
   * 
   * @param key
   * @param value
   * @param numTasks
   * @return a number between 0 and numTasks (exclusive) that tells which
   *         partition it belongs to.
   */
  @Override
  public int getPartition(K key, V value, int numTasks) {
    int returnVal = 0;
    try {

      if ((application != null) && (application.getDownlink() != null)) {
        returnVal = application.getDownlink()
            .getPartition(key, value, numTasks);
      } else {
        LOG.warn("PipesApplication or application.getDownlink() might be null! (application==null): "
            + ((application == null) ? "true" : "false"));
      }

    } catch (IOException e) {
      LOG.error(e);
    }
    LOG.debug("getPartition returns: " + returnVal);
    return returnVal;
  }
  
}
