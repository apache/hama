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
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class TestAsyncRPC extends TestCase {
  private static final int PORT = 1234;
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG = LogFactory
      .getLog("org.apache.hama.ipc.TestAsyncRPC");

  private static Configuration conf = new Configuration();

  public TestAsyncRPC(String name) {
    super(name);
  }

  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;

    void ping() throws IOException;

    String echo(String value) throws IOException;

    String[] echo(String[] value) throws IOException;

    Writable echo(Writable value) throws IOException;

    int add(int v1, int v2) throws IOException;

    int add(int[] values) throws IOException;

    int error() throws IOException;

    void testServerGet() throws IOException;
  }

  public class TestImpl implements TestProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.versionID;
    }

    @Override
    public void ping() {
    }

    @Override
    public String echo(String value) throws IOException {
      return value;
    }

    @Override
    public String[] echo(String[] values) throws IOException {
      return values;
    }

    @Override
    public Writable echo(Writable writable) {
      return writable;
    }

    @Override
    public int add(int v1, int v2) {
      return v1 + v2;
    }

    @Override
    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    @Override
    public int error() throws IOException {
      throw new IOException("bobo");
    }

    @Override
    public void testServerGet() throws IOException {
      AsyncServer server = AsyncServer.get();
      if (!(server instanceof AsyncRPC.NioServer)) {
        throw new IOException("ServerWithNetty.get() failed");
      }
    }
  }

  public void testCalls() throws Exception {
    if(!SystemUtils.IS_OS_LINUX) {
      System.out.println("Skipping testcase because Async is only supported for LINUX!");
      return;
    }
    
    AsyncServer server = AsyncRPC
        .getServer(new TestImpl(), ADDRESS, PORT, conf);
    server.start();

    InetSocketAddress addr = new InetSocketAddress(PORT);
    TestProtocol proxy = (TestProtocol) AsyncRPC.getProxy(TestProtocol.class,
        TestProtocol.versionID, addr, conf);

    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    stringResult = proxy.echo((String) null);
    assertEquals(stringResult, null);

    String[] stringResults = proxy.echo(new String[] { "foo", "bar" });
    assertTrue(Arrays.equals(stringResults, new String[] { "foo", "bar" }));

    stringResults = proxy.echo((String[]) null);
    assertTrue(Arrays.equals(stringResults, null));

    int intResult = proxy.add(1, 2);
    assertEquals(intResult, 3);

    intResult = proxy.add(new int[] { 1, 2 });
    assertEquals(intResult, 3);

    boolean caught = false;
    try {
      proxy.error();
    } catch (Exception e) {
      LOG.debug("Caught " + e);
      caught = true;
    }
    assertTrue(caught);

    proxy.testServerGet();

    // try some multi-calls
    Method echo = TestProtocol.class.getMethod("echo",
        new Class[] { String.class });
    String[] strings = (String[]) AsyncRPC.call(echo, new String[][] { { "a" },
        { "b" } }, new InetSocketAddress[] { addr, addr }, null, conf);
    assertTrue(Arrays.equals(strings, new String[] { "a", "b" }));

    Method ping = TestProtocol.class.getMethod("ping", new Class[] {});
    Object[] voids = AsyncRPC.call(ping, new Object[][] { {}, {} },
        new InetSocketAddress[] { addr, addr }, null, conf);
    assertEquals(voids, null);

    server.stop();
  }

  public static void main(String[] args) throws Exception {
    new TestAsyncRPC("test").testCalls();
  }

}
