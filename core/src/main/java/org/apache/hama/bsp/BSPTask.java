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
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.ipc.BSPPeerProtocol;

/**
 * Base class for tasks.
 */
public final class BSPTask extends Task {

  public static final Log LOG = LogFactory.getLog(BSPTask.class);

  private BSPJob conf;
  BytesWritable split;
  String splitClass;

  public BSPTask() {
  }

  public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid,
      int partition, String splitClass, BytesWritable split) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.taskId = taskid;
    this.partition = partition;

    this.splitClass = splitClass;
    this.split = split;
  }

  @Override
  public final BSPTaskRunner createRunner(GroomServer groom) {
    return new BSPTaskRunner(this, groom, this.conf);
  }

  @Override
  public final void run(BSPJob job, BSPPeerImpl<?, ?, ?, ?> bspPeer,
      BSPPeerProtocol umbilical) throws IOException, SyncException,
      ClassNotFoundException, InterruptedException {
    runBSP(job, bspPeer, split, umbilical);

    done(umbilical);
  }

  @SuppressWarnings("unchecked")
  private final <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void runBSP(
      final BSPJob job, BSPPeerImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> bspPeer,
      final BytesWritable rawSplit, final BSPPeerProtocol umbilical)
      throws IOException, SyncException, ClassNotFoundException,
      InterruptedException {

    BSP<KEYIN, VALUEIN, KEYOUT, VALUEOUT> bsp = (BSP<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) ReflectionUtils
        .newInstance(job.getConf().getClass("bsp.work.class", BSP.class),
            job.getConf());

    bsp.setup(bspPeer);
    bsp.bsp(bspPeer);

    bsp.cleanup(bspPeer);
    bspPeer.close();
  }

  public final BSPJob getConf() {
    return conf;
  }

  public final void setConf(BSPJob conf) {
    this.conf = conf;
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    super.write(out);
    if (split != null) {
      out.writeBoolean(true);
      Text.writeString(out, splitClass);
      split.write(out);
      split = null;
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (in.readBoolean()) {
      splitClass = Text.readString(in);
      if (split == null) {
        split = new BytesWritable();
      }
      split.readFields(in);
    }
  }

}
