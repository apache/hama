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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class BSPMessageSerializer {

  private static final Log LOG = LogFactory.getLog(BSPMessageSerializer.class);

  final Socket client;
  final ScheduledExecutorService sched;
  final Configuration conf;

  public BSPMessageSerializer(final Configuration conf, final int port) {
    this.conf = conf;
    Socket tmp = null;
    int cnt = 0;
    do {
      tmp = init(port);
      cnt++;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.warn("Thread is interrupted.", ie);
        Thread.currentThread().interrupt();
      }
    } while (null == tmp && 10 > cnt);
    this.client = tmp;
    if (null == this.client)
      throw new NullPointerException("Client socket is null.");
    this.sched = Executors.newScheduledThreadPool(conf.getInt(
        "bsp.checkpoint.serializer_thread", 10));
    LOG.info(BSPMessageSerializer.class.getName()
        + " is ready to serialize message.");
  }

  private Socket init(final int port) {
    Socket tmp = null;
    try {
      tmp = new Socket("localhost", port);
    } catch (UnknownHostException uhe) {
      LOG.error("Unable to connect to BSPMessageDeserializer.", uhe);
    } catch (IOException ioe) {
      LOG.warn("Fail to create socket.", ioe);
    }
    return tmp;
  }

  void serialize(final BSPSerializableMessage tmp) throws IOException {
    if (LOG.isDebugEnabled())
      LOG.debug("Messages are saved to " + tmp.checkpointedPath());
    final DataOutput out = new DataOutputStream(client.getOutputStream());
    this.sched.schedule(new Callable<Object>() {
      public Object call() throws Exception {
        tmp.write(out);
        return null;
      }
    }, 0, SECONDS);
  }

  public void close() {
    try {
      this.client.close();
      this.sched.shutdown();
    } catch (IOException io) {
      LOG.error("Fail to close client socket.", io);
    }
  }

}
