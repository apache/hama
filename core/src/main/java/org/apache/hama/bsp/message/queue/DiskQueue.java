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
package org.apache.hama.bsp.message.queue;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * A disk based queue that is backed by a raw file on local disk. <br/>
 * Structure is as follows: <br/>
 * If "bsp.disk.queue.dir" is not defined, "hama.tmp.dir" will be used instead. <br/>
 * ${hama.tmp.dir}/diskqueue/job_id/task_attempt_id/ <br/>
 * An ongoing sequencenumber will be appended to prevent inner collisions,
 * however the job_id dir will never be deleted. So you need a cronjob to do the
 * cleanup for you. <br/>
 * It is recommended to use the file:// scheme in front of the property, because
 * writes on DFS are expensive, however your local disk may not have enough
 * space for your message, so you can easily switch per job via your
 * configuration. <br/>
 * <b>It is experimental to use.</b>
 */
public final class DiskQueue<M extends Writable> extends POJOMessageQueue<M> {

  public static final String DISK_QUEUE_PATH_KEY = "bsp.disk.queue.dir";

  private static final int MAX_RETRIES = 4;
  private static final Log LOG = LogFactory.getLog(DiskQueue.class);

  private static volatile int ONGOING_SEQUENCE_NUMBER = 0;

  private int size = 0;
  // injected via reflection
  private Configuration conf;
  private FileSystem fs;

  private FSDataOutputStream writer;
  private FSDataInputStream reader;

  private Path queuePath;
  private TaskAttemptID id;
  private final ObjectWritable writable = new ObjectWritable();

  @Override
  public void init(Configuration conf, TaskAttemptID id) {
    this.id = id;
    writable.setConf(conf);
    try {
      fs = FileSystem.get(conf);
      String configuredQueueDir = conf.get(DISK_QUEUE_PATH_KEY);
      Path queueDir = null;
      queueDir = getQueueDir(conf, id, configuredQueueDir);
      fs.mkdirs(queueDir);
      queuePath = new Path(queueDir, (ONGOING_SEQUENCE_NUMBER++)
          + "_messages.seq");
      prepareWrite();
    } catch (IOException e) {
      // we can't recover if something bad happens here..
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    closeInternal(true);
    // try {
    // fs.delete(queuePath.getParent(), true);
    // } catch (IOException e) {
    // LOG.error(e);
    // }
  }

  /**
   * Close our writer internal, basically should be called after the computation
   * phase ended.
   */
  private void closeInternal(boolean delete) {
    try {
      if (writer != null) {
        writer.flush();
        writer.close();
        writer = null;
      }
    } catch (IOException e) {
      LOG.error(e);
    } finally {
      if (fs != null && delete) {
        try {
          fs.delete(queuePath, true);
        } catch (IOException e) {
          LOG.error(e);
        }
      }
      if (writer != null) {
        try {
          writer.flush();
          writer.close();
          writer = null;
        } catch (IOException e) {
          LOG.error(e);
        }
      }
    }
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (IOException e) {
      LOG.error(e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
          reader = null;
        } catch (IOException e) {
          LOG.error(e);
        }
      }
    }
  }

  @Override
  public void prepareRead() {
    // make sure we've closed
    closeInternal(false);
    try {
      reader = fs.open(queuePath);
    } catch (IOException e) {
      // can't recover from that
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void prepareWrite() {
    try {
      writer = fs.create(queuePath);
    } catch (IOException e) {
      // can't recover from that
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public final void addAll(Iterable<M> col) {
    for (M item : col) {
      add(item);
    }
  }

  @Override
  public void addAll(MessageQueue<M> otherqueue) {
    M poll = null;
    while ((poll = otherqueue.poll()) != null) {
      add(poll);
    }
  }

  @Override
  public final void add(M item) {
    size++;
    try {
      new ObjectWritable(item).write(writer);
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  @Override
  public final void clear() {
    closeInternal(true);
    size = 0;
    init(conf, id);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final M poll() {
    if (size == 0) {
      return null;
    }
    size--;
    int tries = 1;
    while (tries <= MAX_RETRIES) {
      try {
        writable.readFields(reader);
        if (size > 0) {
          return (M) writable.get();
        } else {
          closeInternal(true);
          return (M) writable.get();
        }
      } catch (IOException e) {
        LOG.error("Retrying for the " + tries + "th time!", e);
      }
      tries++;
    }
    throw new RuntimeException("Message couldn't be read for " + tries
        + " times! Giving up!");
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Iterator<M> iterator() {
    return new DiskIterator();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private class DiskIterator implements Iterator<M> {

    public DiskIterator() {
      prepareRead();
    }

    @Override
    public boolean hasNext() {
      if (size == 0) {
        closeInternal(false);
      }
      return size != 0;
    }

    @Override
    public M next() {
      return poll();
    }

    @Override
    public void remove() {
      // no-op
    }

  }

  /**
   * Creates a path for a queue
   */
  public static Path getQueueDir(Configuration conf, TaskAttemptID id,
      String configuredQueueDir) {
    Path queueDir;
    if (configuredQueueDir == null) {
      String hamaTmpDir = conf.get("hama.tmp.dir");
      if (hamaTmpDir != null) {
        queueDir = createDiskQueuePath(id, hamaTmpDir);
      } else {
        // use some local tmp dir
        queueDir = createDiskQueuePath(id, "/tmp/messageStorage/");
      }
    } else {
      queueDir = createDiskQueuePath(id, configuredQueueDir);
    }
    return queueDir;
  }

  /**
   * Creates a generic Path based on the configured path and the task attempt id
   * to store disk sequence files. <br/>
   * Structure is as follows: ${hama.tmp.dir}/diskqueue/job_id/task_attempt_id/
   */
  private static Path createDiskQueuePath(TaskAttemptID id,
      String configuredPath) {
    return new Path(new Path(new Path(configuredPath, "diskqueue"), id
        .getJobID().toString()), id.getTaskID().toString());
  }

  @Override
  public boolean isMessageSerialized() {
    return false;
  }

  @Override
  public boolean isMemoryBasedQueue() {
    return false;
  }
}
