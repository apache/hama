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
package org.apache.hama.ml.kcore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class KCoreVertex extends
    Vertex<LongWritable, LongWritable, KCoreMessage> {

  private static final Logger LOG = Logger.getLogger(KCoreVertex.class);
  private int core;
  private boolean changed;
  private HashMap<Long, Integer> estimates;

  public KCoreVertex() {
    super();
    this.changed = false;
    this.core = 0;
    this.estimates = new HashMap<Long, Integer>();
  }

  public KCoreVertex(int core) {
    super();
    this.changed = false;
    this.core = core;
    this.estimates = new HashMap<Long, Integer>();
  }

  public KCoreVertex(int core, HashMap<Long, Integer> estimates) {
    super();
    this.changed = false;
    this.core = core;
    this.estimates = estimates;
  }

  public boolean isChanged() {
    return changed;
  }

  public void setChanged(boolean changed) {
    this.changed = changed;
  }

  public int getCore() {
    return core;
  }

  public void setCore(int core) {
    this.core = core;
  }

  public double getNeighborEstimate(long neighbor) {
    if (estimates.containsKey(neighbor)) {
      return estimates.get(neighbor);
    }
    return (-1);
  }

  public void setNeighborNewEstimate(long neighbor, int estimate) {
    if (this.estimates.containsKey(neighbor)) {
      this.estimates.put(neighbor, estimate);
    }
  }

  public void logNeighborEstimates(long vertexId) {
    LOG.info(vertexId + " neighbor estimates: ");
    for (Map.Entry<Long, Integer> entry : estimates.entrySet()) {
      LOG.info("\t" + entry.getKey() + "-" + entry.getValue());
    }
  }

  public void setNeighborEstimate(long neighbor, int estimate) {
    estimates.put(neighbor, estimate);
  }

  @Override
  public void readState(DataInput in) throws IOException {
    core = in.readInt();
    changed = in.readBoolean();

    this.estimates = new HashMap<Long, Integer>();
    if (in.readBoolean()) {
      int num = in.readInt();
      for (int i = 0; i < num; ++i) {
        long key = in.readLong();
        int value = in.readInt();
        estimates.put(key, value);
      }
    }
  }

  @Override
  public void writeState(DataOutput out) throws IOException {
    out.writeInt(core);
    out.writeBoolean(changed);

    if (this.estimates == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(this.estimates.size());

      for (Map.Entry<Long, Integer> entry : estimates.entrySet()) {
        out.writeLong(entry.getKey());
        out.writeInt(entry.getValue());
      }
    }
  }

  public int computeEstimate() {
    int old = this.core;
    double[] count = new double[this.core + 1];

    for (Map.Entry<Long, Integer> entry : this.estimates.entrySet()) {
      LOG.info("Processing " + entry.getKey() + ": " + entry.getValue());
      double j = Math.min(this.core, entry.getValue().doubleValue());
      LOG.info("Min: " + j);
      count[(int) j] = count[(int) j] + 1;
    }

    LOG.info("Count before");
    int i;
    for (i = 0; i < count.length; i++) {
      LOG.info(i + " " + count[i]);
    }

    for (i = this.core; i > 1; i--)
      count[i - 1] = count[i - 1] + count[i];

    LOG.info("Count after");
    for (i = 0; i < count.length; i++) {
      LOG.info(i + " " + count[i]);
    }

    i = this.core;
    while ((i > 1) && (count[i] < i)) {
      LOG.info("Decrementing" + i + " down one because " + count[i]
          + " is less than that");
      i = i - 1;
    }
    LOG.info("Loop terminated: i: " + i + " and count[i] = " + count[i]);

    if (i != old) {
      LOG.info("New Core Estimate: " + i + "\n");
    }
    return i;
  }

  @Override
  public void compute(Iterable<KCoreMessage> msgs) throws IOException {
    if (this.getSuperstepCount() == 0) {
      this.core = getEdges().size();

      for (Edge<LongWritable, LongWritable> edge : getEdges()) {
        sendMessage(edge, new KCoreMessage(getVertexID().get(), getCore()));
      }

    } else {
      LOG.info("getSuperstepCount = " + getSuperstepCount() + " vertex = "
          + getVertexID() + " Core = " + getCore());

      List<KCoreMessage> messages = Lists.newArrayList(msgs);
      if (this.getSuperstepCount() == 1) {
        for (KCoreMessage message : messages) {
          estimates.put(message.getVertexID(), (Integer.MAX_VALUE));
        }
      }

      LOG.info(getVertexID() + " got estimates of: ");
      for (KCoreMessage message : messages) {

        LOG.info("Processing message from " + message.getVertexID());

        double temp = getNeighborEstimate(message.getVertexID());

        if (message.getCore() < temp) {
          setNeighborNewEstimate(message.getVertexID(), message.getCore());

          int t = computeEstimate();

          if (t < getCore()) {
            LOG.info("Setting new core value! \n\n");
            setCore(t);
            setChanged(true);
          }
        }
      }
      LOG.info("Done recomputing estimate for node " + getVertexID());

      if (!isChanged()) {
        this.voteToHalt();
      } else {
        for (Edge<LongWritable, LongWritable> edge : getEdges()) {
          sendMessage(edge, new KCoreMessage(getVertexID().get(), getCore()));
        }
        setChanged(false);
      }
    }
  }

}
