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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hama.Constants;
import org.apache.hama.ipc.Server;

/**
 * NetUtils for our needs.
 */
public class BSPNetUtils {
  public static final Log LOG = LogFactory.getLog(BSPNetUtils.class);

  public static final int MAX_PORT_NUMBER = 65535;

  private static Map<String, String> hostToResolved = new HashMap<String, String>();

  /**
   * Gets the canonical hostname of this machine.
   * 
   * @return String representing host canonical host name
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
  public static int getFreePort(int pStartPort) {
    int startPort = pStartPort;
    while (!available(startPort)) {
      startPort++;
    }
    return startPort;
  }

  /**
   * Gets a new InetSocketAddress from the given peerName. peerName must contain
   * a colon to distinct between host and port.
   * 
   * @param peerName the name as a String of the BSP peer to get the address
   *          from
   * @return the InetSocketAddress of the given BSP peer
   */
  public static InetSocketAddress getAddress(String peerName) {
    int index = peerName.lastIndexOf(':');
    if (index <= 0 || index == peerName.length() - 1) {
      throw new ArrayIndexOutOfBoundsException(
          "Invalid host and port information. "
              + "Peername must consist of atleast ONE \":\"! "
              + "Given peername was: " + peerName);
    }
    return new InetSocketAddress(peerName.substring(0, index),
        Integer.valueOf(peerName.substring(index + 1)));
  }

  /**
   * Checks to see if a specific port is available.
   * 
   * @param port the port to check for availability
   */
  public static boolean available(int port) {
    if (port < 1000 || port > MAX_PORT_NUMBER) {
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

  /**
   * Returns InetSocketAddress that a client can use to connect to the server.
   * Server.getListenerAddress() is not correct when the server binds to
   * "0.0.0.0". This returns "127.0.0.1:port" when the getListenerAddress()
   * returns "0.0.0.0:port".
   * 
   * @param server
   * @return socket address that a client can use to connect to the server.
   */
  public static InetSocketAddress getConnectAddress(Server server) {
    InetSocketAddress addr = server.getListenerAddress();
    if (addr.getAddress().isAnyLocalAddress()) {
      addr = makeSocketAddr("127.0.0.1", addr.getPort());
    }
    return addr;
  }

  /**
   * Create a socket address with the given host and port. The hostname might be
   * replaced with another host that was set via
   * {@link #addStaticResolution(String, String)}. The value of
   * hadoop.security.token.service.use_ip will determine whether the standard
   * java host resolver is used, or if the fully qualified resolver is used.
   * 
   * @param host the hostname or IP use to instantiate the object
   * @param port the port number
   * @return InetSocketAddress
   */
  public static InetSocketAddress makeSocketAddr(String host, int port) {
    String staticHost = getStaticResolution(host);
    String resolveHost = (staticHost != null) ? staticHost : host;

    InetSocketAddress addr;
    try {
      InetAddress iaddr = SecurityUtil.getByName(resolveHost);
      // if there is a static entry for the host, make the returned
      // address look like the original given host
      if (staticHost != null) {
        iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
      }
      addr = new InetSocketAddress(iaddr, port);
    } catch (UnknownHostException e) {
      addr = InetSocketAddress.createUnresolved(host, port);
    }
    return addr;
  }

  /**
   * Retrieves the resolved name for the passed host. The resolved name must
   * have been set earlier using
   * {@link NetUtils#addStaticResolution(String, String)}
   * 
   * @param host
   * @return the resolution
   */
  public static String getStaticResolution(String host) {
    synchronized (hostToResolved) {
      return hostToResolved.get(host);
    }
  }

  /**
   * Handle the transition from pairs of attributes specifying a host and port
   * to a single colon separated one.
   * 
   * @param conf the configuration to check
   * @param oldBindAddressName the old address attribute name
   * @param oldPortName the old port attribute name
   * @param newBindAddressName the new combined name
   * @return the complete address from the configuration
   */
  public static String getServerAddress(Configuration conf,
      String oldBindAddressName, String oldPortName, String newBindAddressName) {
    String oldAddr = conf.get(oldBindAddressName);
    String oldPort = conf.get(oldPortName);
    String newAddrPort = conf.get(newBindAddressName);
    if (oldAddr == null && oldPort == null) {
      return newAddrPort;
    }
    String[] newAddrPortParts = newAddrPort.split(":", 2);
    if (newAddrPortParts.length != 2) {
      throw new IllegalArgumentException("Invalid address/port: " + newAddrPort);
    }
    if (oldAddr == null) {
      oldAddr = newAddrPortParts[0];
    } else {
      LOG.warn("Configuration parameter " + oldBindAddressName
          + " is deprecated. Use " + newBindAddressName + " instead.");
    }
    if (oldPort == null) {
      oldPort = newAddrPortParts[1];
    } else {
      LOG.warn("Configuration parameter " + oldPortName
          + " is deprecated. Use " + newBindAddressName + " instead.");
    }
    return oldAddr + ":" + oldPort;
  }

  /**
   * Util method to build socket addr from either: <host>:<post>
   * <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target) {
    return createSocketAddr(target, -1);
  }

  /**
   * Util method to build socket addr from either: <host> <host>:<post>
   * <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target,
      int defaultPort) {
    if (target == null) {
      throw new IllegalArgumentException("Socket address is null");
    }
    boolean hasScheme = target.contains("://");
    URI uri = null;
    try {
      uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://"
          + target);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target);
    }

    String host = uri.getHost();
    int port = uri.getPort();
    if (port == -1) {
      port = defaultPort;
    }
    String path = uri.getPath();

    if ((host == null) || (port < 0)
        || (!hasScheme && path != null && !path.isEmpty())) {
      throw new IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target);
    }
    return makeSocketAddr(host, port);
  }

  /**
   * Checks if {@code host} is a local host name and return {@link InetAddress}
   * corresponding to that address.
   * 
   * @param host the specified host
   * @return a valid local {@link InetAddress} or null
   * @throws SocketException if an I/O error occurs
   */
  public static InetAddress getLocalInetAddress(String host)
      throws SocketException {
    if (host == null) {
      return null;
    }
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(host);
      if (NetworkInterface.getByInetAddress(addr) == null) {
        addr = null; // Not a local address
      }
    } catch (UnknownHostException ignore) {
    }
    return addr;
  }

  /**
   * This is a drop-in replacement for
   * {@link Socket#connect(SocketAddress, int)}. In the case of normal sockets
   * that don't have associated channels, this just invokes
   * <code>socket.connect(endpoint, timeout)</code>. If
   * <code>socket.getChannel()</code> returns a non-null channel, connect is
   * implemented using Hadoop's selectors. This is done mainly to avoid Sun's
   * connect implementation from creating thread-local selectors, since Hadoop
   * does not have control on when these are closed and could end up taking all
   * the available file descriptors.
   * 
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   * 
   * @param socket
   * @param address the remote address
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket, SocketAddress address, int timeout)
      throws IOException {
    connect(socket, address, null, timeout);
  }

  /**
   * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but also takes a
   * local address and port to bind the socket to.
   * 
   * @param socket
   * @param endpoint the remote address
   * @param localAddr the local address to bind the socket to
   * @param timeout timeout in milliseconds
   */
  public static void connect(Socket socket, SocketAddress endpoint,
      SocketAddress localAddr, int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }

    SocketChannel ch = socket.getChannel();

    if (localAddr != null) {
      socket.bind(localAddr);
    }

    if (ch == null) {
      // let the default implementation handle it.
      socket.connect(endpoint, timeout);
    } else {
      SocketIOWithTimeout.connect(ch, endpoint, timeout);
    }

    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort()
        && socket.getLocalAddress().equals(socket.getInetAddress())) {
      LOG.info("Detected a loopback TCP socket, disconnecting it");
      socket.close();
      throw new ConnectException(
          "Localhost targeted connection resulted in a loopback. "
              + "No daemon is listening on the target port.");
    }
  }

  /**
   * Same as getInputStream(socket, socket.getSoTimeout()).<br>
   * <br>
   * 
   * From documentation for {@link #getInputStream(Socket, long)}:<br>
   * Returns InputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a {@link SocketInputStream} with the given
   * timeout. If the socket does not have a channel,
   * {@link Socket#getInputStream()} is returned. In the later case, the timeout
   * argument is ignored and the timeout set with
   * {@link Socket#setSoTimeout(int)} applies for reads.<br>
   * <br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   * 
   * @see #getInputStream(Socket, long)
   * 
   * @param socket
   * @return InputStream for reading from the socket.
   * @throws IOException
   */
  public static InputStream getInputStream(Socket socket) throws IOException {
    return getInputStream(socket, socket.getSoTimeout());
  }

  /**
   * Returns InputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a {@link SocketInputStream} with the given
   * timeout. If the socket does not have a channel,
   * {@link Socket#getInputStream()} is returned. In the later case, the timeout
   * argument is ignored and the timeout set with
   * {@link Socket#setSoTimeout(int)} applies for reads.<br>
   * <br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero for
   *          waiting as long as necessary.
   * @return InputStream for reading from the socket.
   * @throws IOException
   */
  @SuppressWarnings("resource")
  public static InputStream getInputStream(Socket socket, long timeout)
      throws IOException {
    return (socket.getChannel() == null) ? socket.getInputStream()
        : new SocketInputStream(socket, timeout);
  }

  /**
   * Same as getOutputStream(socket, 0). Timeout of zero implies write will wait
   * until data is available.<br>
   * <br>
   * 
   * From documentation for {@link #getOutputStream(Socket, long)} : <br>
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a {@link SocketOutputStream} with the given
   * timeout. If the socket does not have a channel,
   * {@link Socket#getOutputStream()} is returned. In the later case, the
   * timeout argument is ignored and the write will wait until data is
   * available.<br>
   * <br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see #getOutputStream(Socket, long)
   * 
   * @param socket
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */
  public static OutputStream getOutputStream(Socket socket) throws IOException {
    return getOutputStream(socket, 0);
  }

  /**
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a {@link SocketOutputStream} with the given
   * timeout. If the socket does not have a channel,
   * {@link Socket#getOutputStream()} is returned. In the later case, the
   * timeout argument is ignored and the write will wait until data is
   * available.<br>
   * <br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero for
   *          waiting as long as necessary.
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */
  @SuppressWarnings("resource")
  public static OutputStream getOutputStream(Socket socket, long timeout)
      throws IOException {
    return (socket.getChannel() == null) ? socket.getOutputStream()
        : new SocketOutputStream(socket, timeout);
  }

  /**
   * Get the default socket factory as specified by the configuration parameter
   * <tt>hadoop.rpc.socket.factory.default</tt>
   * 
   * @param conf the configuration
   * @return the default socket factory as specified in the configuration or the
   *         JVM default socket factory if the configuration does not contain a
   *         default socket factory property.
   */
  public static SocketFactory getDefaultSocketFactory(Configuration conf) {

    String propValue = conf.get("hadoop.rpc.socket.factory.class.default");
    if ((propValue == null) || (propValue.length() == 0))
      return SocketFactory.getDefault();

    return getSocketFactoryFromProperty(conf, propValue);
  }

  /**
   * Get the socket factory corresponding to the given proxy URI. If the given
   * proxy URI corresponds to an absence of configuration parameter, returns
   * null. If the URI is malformed raises an exception.
   * 
   * @param propValue the property which is the class name of the SocketFactory
   *          to instantiate; assumed non null and non empty.
   * @return a socket factory as defined in the property value.
   */
  public static SocketFactory getSocketFactoryFromProperty(Configuration conf,
      String propValue) {

    try {
      Class<?> theClass = conf.getClassByName(propValue);
      return (SocketFactory) org.apache.hadoop.util.ReflectionUtils
          .newInstance(theClass, conf);

    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Socket Factory class not found: " + cnfe);
    }
  }
}
