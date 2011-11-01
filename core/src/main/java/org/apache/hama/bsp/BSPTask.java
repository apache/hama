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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.zookeeper.KeeperException;

/**
 * Base class for tasks.
 */
public class BSPTask extends Task {

  public static final Log LOG = LogFactory.getLog(BSPTask.class);

  private BSPJob conf;
  private BytesWritable split = new BytesWritable();
  private String splitClass;

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
  public BSPTaskRunner createRunner(GroomServer groom) {
    return new BSPTaskRunner(this, groom, this.conf);
  }

  @Override
  public void run(BSPJob job, BSPPeerImpl bspPeer, BSPPeerProtocol umbilical)
      throws IOException {

    try {
      runBSP(job, bspPeer, split, umbilical);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    done(umbilical);
  }

  @SuppressWarnings("unchecked")
  private <INK, INV, OUTK, OUTV> void runBSP(final BSPJob job, BSPPeerImpl bspPeer,
      final BytesWritable rawSplit, final BSPPeerProtocol umbilical)
      throws IOException, InterruptedException, ClassNotFoundException {
    InputSplit inputSplit = null;
    // reinstantiate the split
    try {
      inputSplit = (InputSplit) ReflectionUtils.newInstance(job.getConf()
          .getClassByName(splitClass), job.getConf());
    } catch (ClassNotFoundException exp) {
      IOException wrap = new IOException("Split class " + splitClass
          + " not found");
      wrap.initCause(exp);
      throw wrap;
    }

    DataInputBuffer splitBuffer = new DataInputBuffer();
    splitBuffer.reset(split.getBytes(), 0, split.getLength());
    inputSplit.readFields(splitBuffer);

    RecordReader<INK, INV> in = job.getInputFormat().getRecordReader(
        inputSplit, job);
    
    FileSystem fs = FileSystem.get(job.getConf());
    String finalName = getOutputName(getPartition());
    
    final RecordWriter<OUTK, OUTV> out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName);
    
    OutputCollector<OUTK,OUTV> collector = 
      new OutputCollector<OUTK,OUTV>() {
        public void collect(OUTK key, OUTV value)
          throws IOException {
          out.write(key, value);
        }
      };
      
    BSP bsp = (BSP) ReflectionUtils.newInstance(job.getConf().getClass(
        "bsp.work.class", BSP.class), job.getConf());

    try {
      bsp.setup(bspPeer);
      bsp.bsp(bspPeer, in, collector);
    } catch (IOException e) {
      LOG.error("Exception during BSP execution!", e);
    } catch (KeeperException e) {
      LOG.error("Exception during BSP execution!", e);
    } catch (InterruptedException e) {
      LOG.error("Exception during BSP execution!", e);
    } finally {
      bsp.cleanup(bspPeer);
      out.close();
    }

  }

  public BSPJob getConf() {
    return conf;
  }

  public void setConf(BSPJob conf) {
    this.conf = conf;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, splitClass);
    split.write(out);
    split = null;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    splitClass = Text.readString(in);
    split.readFields(in);
  }

}
