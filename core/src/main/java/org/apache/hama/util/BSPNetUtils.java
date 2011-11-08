/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.mina.util.AvailablePortFinder;

/**
 * NetUtils for our needs.
 */
public class BSPNetUtils {

  /**
   * Gets the canonical hostname of this machine.
   * 
   * @return
   * @throws UnknownHostException
   */
  public static String getCanonicalHostname() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * Uses Minas AvailablePortFinder to find a port, starting at 14000.
   * 
   * @return a free port.
   */
  public static int getFreePort() {
    int startPort = 14000;
    return getFreePort(startPort);
  }

  /**
   * Uses Minas AvailablePortFinder to find a port, starting at startPort.
   * 
   * @return a free port.
   */
  public static int getFreePort(int startPort) {
    while (!AvailablePortFinder.available(startPort)) {
      startPort++;
    }
    return startPort;
  }

  /**
   * Gets a new InetSocketAddress from the given peerName. peerName must contain
   * a colon to distinct between host and port.
   * 
   * @param peerName
   * @return
   */
  public static InetSocketAddress getAddress(String peerName) {
    String[] peerAddrParts = peerName.split(":");
    if (peerAddrParts.length != 2) {
      throw new ArrayIndexOutOfBoundsException(
          "Peername must consist of exactly ONE \":\"! Given peername was: "
              + peerName);
    }
    return new InetSocketAddress(peerAddrParts[0],
        Integer.valueOf(peerAddrParts[1]));
  }

}
