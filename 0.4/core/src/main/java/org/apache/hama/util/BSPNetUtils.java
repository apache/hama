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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;

import org.apache.hama.Constants;
import org.apache.mina.util.AvailablePortFinder;

/**
 * NetUtils for our needs.
 */
public class BSPNetUtils {
  public static final int MAX_PORT_NUMBER = 65535;

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

  /**
   * Checks to see if a specific port is available.
   * 
   * @param port the port to check for availability
   */
  public static boolean available(int port) {
    if (port < Constants.DEFAULT_PEER_PORT || port > MAX_PORT_NUMBER) {
      throw new IllegalArgumentException("Invalid start port: " + port);
    }

    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }

    return false;
  }

  public static int getNextAvailable(int fromPort) {
    if ((fromPort < Constants.DEFAULT_PEER_PORT)
        || (fromPort > MAX_PORT_NUMBER)) {
      throw new IllegalArgumentException("Invalid start port: " + fromPort);
    }

    for (int i = fromPort + 1; i <= MAX_PORT_NUMBER; i++) {
      if (available(i)) {
        return i;
      }
    }

    throw new NoSuchElementException("Could not find an available port "
        + "above " + fromPort);
  }
}
