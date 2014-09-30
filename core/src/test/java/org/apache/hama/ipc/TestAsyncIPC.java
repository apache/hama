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
package org.apache.hama.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class TestAsyncIPC extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestAsyncIPC.class);

  final private static Configuration conf = new Configuration();
  final static private int PING_INTERVAL = 1000;

  static {
    Client.setPingInterval(conf, PING_INTERVAL);
  }

  public TestAsyncIPC(String name) {
    super(name);
  }

  private static final Random RANDOM = new Random();

  private static final String ADDRESS = "0.0.0.0";
  private static int port = 7000;

  private static class TestServer extends AsyncServer {
    private boolean sleep;

    public TestServer(int handlerCount, boolean sleep) throws IOException {
      super(ADDRESS, port++, LongWritable.class, handlerCount, conf);
      this.sleep = sleep;
    }

    @Override
    public Writable call(Class<?> protocol, Writable param, long receiveTime)
        throws IOException {
      if (sleep) {
        try {
          Thread.sleep(RANDOM.nextInt(2 * PING_INTERVAL)); // sleep a bit
        } catch (InterruptedException e) {
        }
      }
      return param; // echo param as result
    }
  }

  private static class SerialCaller extends Thread {
    private AsyncClient client;
    private InetSocketAddress serverAddress;
    private int count;
    private boolean failed;

    public SerialCaller(AsyncClient client, InetSocketAddress server, int count) {
      this.client = client;
      this.serverAddress = server;
      this.count = count;
    }

    @Override
    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          LongWritable param = new LongWritable(RANDOM.nextLong());

          LongWritable value = (LongWritable) client.call(param, serverAddress,
              null, null, 0, conf);
          if (!param.equals(value)) {
            LOG.fatal("Call failed!");
            failed = true;
            break;
          }

        } catch (Exception e) {
          LOG.fatal("Caught : " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }
  }

  private static class ParallelCaller extends Thread {
    private AsyncClient client;
    private int count;
    private InetSocketAddress[] addresses;
    private boolean failed;

    public ParallelCaller(AsyncClient client, InetSocketAddress[] addresses,
        int count) {
      this.client = client;
      this.addresses = addresses;
      this.count = count;
    }

    @Override
    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          Writable[] params = new Writable[addresses.length];
          for (int j = 0; j < addresses.length; j++)
            params[j] = new LongWritable(RANDOM.nextLong());
          Writable[] values = client.call(params, addresses, null, null, conf);

          for (int j = 0; j < addresses.length; j++) {
            if (!params[j].equals(values[j])) {
              LOG.fatal("Call failed!");
              failed = true;
              break;
            }
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }
  }

  public void testSerial() throws Exception {
    testSerial(3, false, 2, 5, 100);
  }

  public void testSerial(int handlerCount, boolean handlerSleep,
      int clientCount, int callerCount, int callCount) throws Exception {
    AsyncServer server = new TestServer(handlerCount, handlerSleep);
    InetSocketAddress addr = server.getAddress();
    server.start();

    AsyncClient[] clients = new AsyncClient[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new AsyncClient(LongWritable.class, conf);
    }

    SerialCaller[] callers = new SerialCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new SerialCaller(clients[i % clientCount], addr, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    server.stop();
  }

  public void testParallel() throws Exception {
    testParallel(10, false, 2, 4, 2, 4, 100);
  }

  public void testParallel(int handlerCount, boolean handlerSleep,
      int serverCount, int addressCount, int clientCount, int callerCount,
      int callCount) throws Exception {
    AsyncServer[] servers = new AsyncServer[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = new TestServer(handlerCount, handlerSleep);
      servers[i].start();
    }

    InetSocketAddress[] addresses = new InetSocketAddress[addressCount];
    for (int i = 0; i < addressCount; i++) {
      addresses[i] = servers[i % serverCount].address;
    }

    AsyncClient[] clients = new AsyncClient[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new AsyncClient(LongWritable.class, conf);
    }

    ParallelCaller[] callers = new ParallelCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new ParallelCaller(clients[i % clientCount], addresses,
          callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    for (int i = 0; i < serverCount; i++) {
      servers[i].stop();
    }
  }
}
