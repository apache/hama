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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.util.BSPNetUtils;

import javax.net.SocketFactory;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A client for an IPC service using netty. IPC calls take a single
 * {@link Writable} as a parameter, and return a {@link Writable} as their
 * value. A service runs on a port and is defined by a parameter class and a
 * value class.
 * 
 * @see AsyncClient
 */
public class AsyncClient {
  private static final String IPC_CLIENT_CONNECT_MAX_RETRIES_KEY = "ipc.client.connect.max.retries";
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT = 10;
  private static final Log LOG = LogFactory.getLog(AsyncClient.class);
  private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();

  private Class<? extends Writable> valueClass; // class of call values
  private int counter = 0; // counter for call ids
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final private Configuration conf; // configuration obj

  private SocketFactory socketFactory; // only use in order to meet the
                                       // consistency with other clients
  private int refCount = 1;

  final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
  final static int DEFAULT_PING_INTERVAL = 60000; // 1 min

  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  final public static void setPingInterval(Configuration conf, int pingInterval) {
    conf.setInt(PING_INTERVAL_NAME, pingInterval);
  }

  /**
   * Get the ping interval from configuration; If not set in the configuration,
   * return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  final static int getPingInterval(Configuration conf) {
    return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
  }

  /**
   * The time after which a RPC will timeout. If ping is not enabled (via
   * ipc.client.ping), then the timeout value is the same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   */
  final public static int getTimeout(Configuration conf) {
    if (!conf.getBoolean("ipc.client.ping", true)) {
      return getPingInterval(conf);
    }
    return -1;
  }

  /**
   * Increment this client's reference count
   * 
   */
  synchronized void incCount() {
    refCount++;
  }

  /**
   * Decrement this client's reference count
   * 
   */
  synchronized void decCount() {
    refCount--;
  }

  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount == 0;
  }

  /**
   * Thread that reads responses and notifies callers. Each connection owns a
   * socket connected to a remote address. Calls are multiplexed through this
   * socket: responses may be delivered out of order.
   */
  private class Connection {
    private InetSocketAddress serverAddress; // server ip:port
    private ConnectionHeader header; // connection header
    private final ConnectionId remoteId; // connection id
    private AuthMethod authMethod; // authentication method

    private EventLoopGroup group;
    private Bootstrap bootstrap;
    private Channel channel;
    private int rpcTimeout;
    private int maxIdleTime; // connections will be culled if it was idle

    private final RetryPolicy connectionRetryPolicy;
    private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private int pingInterval; // how often sends ping to the server in msecs

    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean(); // indicate
    private IOException closeException; // if the connection is closed, close
                                        // reason

    /**
     * Setup Connection Configuration
     * 
     * @param remoteId remote connection Id
     * @throws IOException
     */
    public Connection(ConnectionId remoteId) throws IOException {
      group = new EpollEventLoopGroup();
      bootstrap = new Bootstrap();
      this.remoteId = remoteId;
      this.serverAddress = remoteId.getAddress();
      if (serverAddress.isUnresolved()) {
        throw new UnknownHostException("unknown host: "
            + remoteId.getAddress().getHostName());
      }
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.pingInterval = remoteId.getPingInterval();
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is" + this.pingInterval + "ms.");
      }
      this.rpcTimeout = remoteId.getRpcTimeout();
      Class<?> protocol = remoteId.getProtocol();

      authMethod = AuthMethod.SIMPLE;
      header = new ConnectionHeader(protocol == null ? null
          : protocol.getName(), null, authMethod);
    }

    /**
     * Add a call to this connection's call queue and notify a listener;
     * synchronized. Returns false if called during shutdown.
     * 
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /**
     * Update the server address if the address corresponding to the host name
     * has changed.
     */
    private synchronized boolean updateAddress() throws IOException {
      // Do a fresh lookup with the old host name.
      InetSocketAddress currentAddr = BSPNetUtils.makeSocketAddr(
          serverAddress.getHostName(), serverAddress.getPort());

      if (!serverAddress.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: " + serverAddress.toString()
            + " New: " + currentAddr.toString());
        serverAddress = currentAddr;
        return true;
      }
      return false;
    }

    /**
     * Connect to the server and set up the I/O streams. It then sends a header
     * to the server.
     */
    private void setupIOstreams() throws InterruptedException {
      if (channel != null && channel.isActive()) {
        return;
      }
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to " + serverAddress);
        }

        setupConnection();
        writeHeader();
      } catch (Throwable t) {
        if (t instanceof IOException) {
          markClosed((IOException) t);
        } else {
          markClosed(new IOException("Couldn't set up IO streams", t));
        }
        close();
      }
    }

    /**
     * Configure the client and connect to server
     */
    private void setupConnection() throws Exception {
      while (true) {
        short ioFailures = 0;
        try {
          // rpcTimeout overwrites pingInterval
          if (rpcTimeout > 0) {
            pingInterval = rpcTimeout;
          }

          // Configure the client.
          // EpollEventLoopGroup is a multithreaded event loop that handles I/O
          // operation
          group = new EpollEventLoopGroup();
          // Bootstrap is a helper class that sets up a client
          bootstrap = new Bootstrap();
          bootstrap.group(group).channel(EpollSocketChannel.class)
              .option(ChannelOption.TCP_NODELAY, this.tcpNoDelay)
              .option(ChannelOption.SO_KEEPALIVE, true)
              .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, pingInterval)
              .option(ChannelOption.SO_SNDBUF, 30 * 1024 * 1024)
              .handler(new LoggingHandler(LogLevel.INFO))
              .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline p = ch.pipeline();
                  p.addLast(new IdleStateHandler(0, 0, maxIdleTime));
                  // Register message processing handler
                  p.addLast(new NioClientInboundHandler());
                }
              });

          // Bind and start to accept incoming connections.
          ChannelFuture channelFuture = bootstrap.connect(
              serverAddress.getAddress(), serverAddress.getPort()).sync();
          // Get io channel
          channel = channelFuture.channel();
          LOG.info("AsyncClient startup");
          break;
        } catch (Exception ie) {
          /*
           * Check for an address change and update the local reference. Reset
           * the failure counter if the address was changed
           */

          if (updateAddress()) {
            ioFailures = 0;
          }
          handleConnectionFailure(ioFailures++, ie);
        }
      }
    }

    /**
     * Write the header protocol header for each connection Out is not
     * synchronized because only the first thread does this.
     * 
     * @param channel
     */
    private void writeHeader() {
      DataOutputBuffer rpcBuff = null;
      DataOutputBuffer headerBuf = null;
      try {
        ByteBuf buf = channel.alloc().buffer();
        rpcBuff = new DataOutputBuffer();
        authMethod.write(rpcBuff);

        headerBuf = new DataOutputBuffer();
        header.write(headerBuf);
        byte[] data = headerBuf.getData();
        int dataLength = headerBuf.getLength();
        // write rpcheader
        buf.writeInt(AsyncServer.HEADER_LENGTH + dataLength);
        buf.writeBytes(AsyncServer.HEADER.array());
        buf.writeByte(AsyncServer.CURRENT_VERSION);
        buf.writeByte(rpcBuff.getData()[0]);
        // write header
        buf.writeInt(dataLength);
        buf.writeBytes(data, 0, dataLength);

        channel.writeAndFlush(buf);
      } catch (Exception e) {
        LOG.error("Couldn't send header" + e);
      } finally {
        IOUtils.closeStream(rpcBuff);
        IOUtils.closeStream(headerBuf);
      }
    }

    /**
     * close the current connection gracefully.
     */
    private void closeConnection() {
      try {
        if (!this.group.isTerminated()) {
          this.group.shutdownGracefully();
          LOG.info("client gracefully shutdown");
        }
      } catch (Exception e) {
        LOG.warn("Not able to close a client", e);
      }
    }

    /**
     * This class process received response message from server.
     */
    private class NioClientInboundHandler extends ChannelInboundHandlerAdapter {

      /**
       * Receive a response. This method is called with the received response
       * message, whenever new data is received from a server.
       * 
       * @param ctx
       * @param cause
       */
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf byteBuf = (ByteBuf) msg;
        ByteBufInputStream byteBufInputStream = new ByteBufInputStream(byteBuf);
        DataInputStream in = new DataInputStream(byteBufInputStream);
        while (true) {
          try {
            if (in.available() <= 0)
              break;
            // try to read an id
            int id = in.readInt();

            if (LOG.isDebugEnabled())
              LOG.debug(serverAddress.getHostName() + " got value #" + id);

            Call call = calls.get(id);

            // read call status
            int state = in.readInt();
            if (state == Status.SUCCESS.state) {
              Writable value = ReflectionUtils.newInstance(valueClass, conf);
              value.readFields(in); // read value
              call.setValue(value);
              calls.remove(id);
            } else if (state == Status.ERROR.state) {
              String className = WritableUtils.readString(in);
              byte[] errorBytes = new byte[in.available()];
              in.readFully(errorBytes);
              call.setException(new RemoteException(className, new String(
                  errorBytes)));
              calls.remove(id);
            } else if (state == Status.FATAL.state) {
              // Close the connection
              markClosed(new RemoteException(WritableUtils.readString(in),
                  WritableUtils.readString(in)));
            } else {
              byte[] garbageBytes = new byte[in.available()];
              in.readFully(garbageBytes);
            }
          } catch (IOException e) {
            markClosed(e);
          }
        }
        IOUtils.closeStream(in);
        IOUtils.closeStream(byteBufInputStream);
        ReferenceCountUtil.release(msg);
      }

      /**
       * Ths event handler method is called with a Throwable due to an I/O
       * error. Then, exception is logged and its associated channel is closed
       * here
       * 
       * @param ctx
       * @param cause
       */
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Occured I/O Error : " + cause.getMessage());
        ctx.close();
      }

      /**
       * this method is triggered after a long reading/writing/idle time, it is
       * marked as to be closed, or the client is marked as not running.
       * 
       * @param ctx
       * @param evt
       */
      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
          IdleStateEvent e = (IdleStateEvent) evt;
          if (e.state() != IdleState.ALL_IDLE) {
            if (!calls.isEmpty() && !shouldCloseConnection.get()
                && running.get()) {
              return;
            } else if (shouldCloseConnection.get()) {
              markClosed(null);
            } else if (calls.isEmpty()) { // idle connection closed or stopped
              markClosed(null);
            } else { // get stopped but there are still pending requests
              markClosed((IOException) new IOException()
                  .initCause(new InterruptedException()));
            }
            closeConnection();
          }
        }

      }
    }

    /**
     * Handle connection failures If the current number of retries is equal to
     * the max number of retries, stop retrying and throw the exception;
     * Otherwise backoff 1 second and try connecting again. This Method is only
     * called from inside setupIOstreams(), which is synchronized. Hence the
     * sleep is synchronized; the locks will be retained.
     * 
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    @SuppressWarnings("unused")
    private void handleConnectionFailure(int curRetries, int maxRetries,
        IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }

      // otherwise back off and retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
      }

      LOG.info("Retrying connect to server: " + serverAddress
          + ". Already tried " + curRetries + " time(s); maxRetries="
          + maxRetries);
    }

    /*
     * Handle connection failures If the current number of retries, stop
     * retrying and throw the exception; Otherwise backoff 1 second and try
     * connecting again. This Method is only called from inside
     * setupIOstreams(), which is synchronized. Hence the sleep is synchronized;
     * the locks will be retained.
     * @param curRetries current number of retries
     * @param ioe failure reason
     * @throws Exception if max number of retries is reached
     */
    private void handleConnectionFailure(int curRetries, Exception ioe)
        throws Exception {
      closeConnection();

      final boolean retry;
      try {
        retry = connectionRetryPolicy.shouldRetry(ioe, curRetries);
      } catch (Exception e) {
        throw e instanceof IOException ? (IOException) e : new IOException(e);
      }
      if (!retry) {
        throw ioe;
      }

      LOG.info("Retrying connect to server: " + serverAddress
          + ". Already tried " + curRetries + " time(s); retry policy is "
          + connectionRetryPolicy);
    }

    /**
     * Return the remote address of server
     * 
     * @return remote server address
     */
    public InetSocketAddress getRemoteAddress() {
      return serverAddress;
    }

    /**
     * Initiates a call by sending the parameter to the remote server.
     * 
     * @param sendCall
     */
    public void sendParam(Call sendCall) {
      if (LOG.isDebugEnabled())
        LOG.debug(this.getClass().getName() + " sending #" + sendCall.id);
      DataOutputBuffer buff = null;
      try {
        buff = new DataOutputBuffer();
        buff.writeInt(sendCall.id);
        sendCall.param.write(buff);
        byte[] data = buff.getData();
        int dataLength = buff.getLength();
        ByteBuf buf = channel.alloc().buffer();

        buf.writeInt(dataLength);
        buf.writeBytes(data, 0, dataLength);
        ChannelFuture channelFuture = channel.writeAndFlush(buf);
        if (channelFuture.cause() != null) {
          throw channelFuture.cause();
        }
      } catch (IOException ioe) {
        markClosed(ioe);
      } catch (Throwable t) {
        markClosed(new IOException(t));
      } finally {
        // the buffer is just an in-memory buffer, but it is still
        // polite to close early
        IOUtils.closeStream(buff);
      }
    }

    /**
     * Mark the connection to be closed
     * 
     * @param ioe
     **/
    private synchronized void markClosed(IOException ioe) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = ioe;
        notifyAll();
      }
    }

    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          Connection connection = connections.remove(remoteId);
          connection.closeConnection();
        }
      }

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn("A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + serverAddress + ": "
              + closeException.getMessage(), closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(serverAddress.getHostName() + ": closed");
    }

    /** Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
      while (itor.hasNext()) {
        Call c = itor.next().getValue();
        c.setException(closeException); // local exception
        itor.remove();
      }
    }
  }

  /** A call waiting for a value. */
  private class Call {
    int id; // call id
    Writable param; // parameter
    Writable value; // value, null if error
    IOException error; // exception, null if value
    boolean done; // true when call is done

    protected Call(Writable param) {
      this.param = param;
      synchronized (AsyncClient.this) {
        this.id = counter++;
      }
    }

    /**
     * Indicate when the call is complete and the value or error are available.
     * Notifies by default.
     */
    protected synchronized void callComplete() {
      this.done = true;
      notify(); // notify caller
    }

    /**
     * Set the exception when there is an error. Notify the caller the call is
     * done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      this.error = error;
      this.callComplete();
    }

    /**
     * Set the return value when there is no error. Notify the caller the call
     * is done.
     * 
     * @param value return value of the call.
     */
    public synchronized void setValue(Writable value) {
      this.value = value;
      callComplete();
    }
  }

  /** Call implementation used for parallel calls. */
  private class ParallelCall extends Call {
    private ParallelResults results;
    private int index;

    public ParallelCall(Writable param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    @Override
    /** Deliver result to result collector. */
    protected void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  private static class ParallelResults {
    private Writable[] values;
    private int size;
    private int count;

    public ParallelResults(int size) {
      this.values = new Writable[size];
      this.size = size;
    }

    /**
     * Collect a result.
     * 
     * @param call
     */
    public synchronized void callComplete(ParallelCall call) {
      values[call.index] = call.value; // store the value
      count++; // count it
      if (count == size) // if all values are in
        notify(); // then notify waiting caller
    }
  }

  /**
   * Construct an IPC client whose values are of the given {@link Writable}
   * class.
   * 
   * @param valueClass
   * @param conf
   * @param factory
   */
  public AsyncClient(Class<? extends Writable> valueClass, Configuration conf,
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    // SocketFactory only use in order to meet the consistency with other
    // clients
    this.socketFactory = factory;
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * 
   * @param valueClass
   * @param conf
   */
  public AsyncClient(Class<? extends Writable> valueClass, Configuration conf) {
    // SocketFactory only use in order to meet the consistency with other
    // clients
    this(valueClass, conf, BSPNetUtils.getDefaultSocketFactory(conf));
  }

  /**
   * Return the socket factory of this client
   * 
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    // SocketFactory only use in order to meet the consistency with other
    // clients
    return socketFactory;
  }

  /**
   * Stop all threads related to this client. No further calls may be made using
   * this client.
   */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }

    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.closeConnection();
      }
    }
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
   * timeout and <code>conf</code> as configuration for this connection,
   * returning the value. Throws exceptions if there are network problems or if
   * the remote code threw an exception.
   * 
   * @param param
   * @param addr
   * @param protocol
   * @param ticket
   * @param rpcTimeout
   * @param conf
   * @return Response Writable value
   * @throws InterruptedException
   * @throws IOException
   */
  public Writable call(Writable param, InetSocketAddress addr,
      Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
      Configuration conf) throws InterruptedException, IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(param, remoteId);
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the value. Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * 
   * @param param
   * @param remoteId
   * @return Response Writable value
   * @throws InterruptedException
   * @throws IOException
   */
  public Writable call(Writable param, ConnectionId remoteId)
      throws InterruptedException, IOException {
    Call call = new Call(param);

    Connection connection = getConnection(remoteId, call);

    connection.sendParam(call); // send the parameter
    boolean interrupted = false;

    synchronized (call) {
      int callFailCount = 0;
      while (!call.done) {
        try {
          call.wait(1000); // wait for the result
          // prevent client hang from response error
          if (callFailCount++ == IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT)
            break;
        } catch (InterruptedException ie) {
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();

      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          // use the connection because it will reflect an ip change,
          // unlike
          // the remoteId
          throw wrapException(connection.getRemoteAddress(), call.error);
        }
      } else {
        return call.value;
      }
    }
  }

  /**
   * Take an IOException and the address we were trying to connect to and return
   * an IOException with the input exception as the cause. The new exception
   * provides the stack trace of the place where the exception is thrown and
   * some extra diagnostics information. If the exception is ConnectException or
   * SocketTimeoutException, return a new one of the same type; Otherwise return
   * an IOException.
   * 
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  private IOException wrapException(InetSocketAddress addr,
      IOException exception) {
    if (exception instanceof ConnectException) {
      // connection refused; include the host:port in the error
      return (ConnectException) new ConnectException("Call to " + addr
          + " failed on connection exception: " + exception)
          .initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException) new SocketTimeoutException("Call to "
          + addr + " failed on socket timeout exception: " + exception)
          .initCause(exception);
    } else {
      return (IOException) new IOException("Call to " + addr
          + " failed on local exception: " + exception).initCause(exception);

    }
  }

  /**
   * Makes a set of calls in parallel. Each parameter is sent to the
   * corresponding address. When all values are available, or have timed out or
   * errored, the collected results are returned in an array. The array contains
   * nulls for calls that timed out or errored.
   * 
   * @param params
   * @param addresses
   * @param protocol
   * @param ticket
   * @param conf
   * @return Response Writable value array
   * @throws IOException
   * @throws InterruptedException
   */
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses,
      Class<?> protocol, UserGroupInformation ticket, Configuration conf)
      throws IOException, InterruptedException {
    if (addresses.length == 0)
      return new Writable[0];

    ParallelResults results = new ParallelResults(params.length);
    ConnectionId remoteId[] = new ConnectionId[addresses.length];
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);
        try {
          remoteId[i] = ConnectionId.getConnectionId(addresses[i], protocol,
              ticket, 0, conf);
          Connection connection = getConnection(remoteId[i], call);
          connection.sendParam(call); // send each parameter
        } catch (IOException e) {
          // log errors
          LOG.info("Calling " + addresses[i] + " caught: " + e.getMessage(), e);
          results.size--; // wait for one fewer result
        }
      }

      while (results.count != results.size) {
        try {
          results.wait(); // wait for all results
        } catch (InterruptedException e) {
        }
      }

      return results.values;
    }
  }

  // for unit testing only
  Set<ConnectionId> getConnectionIds() {
    synchronized (connections) {
      return connections.keySet();
    }
  }

  /**
   * Get a connection from the pool, or create a new one and add it to the pool.
   * Connections to a given ConnectionId are reused.
   * 
   * @param remoteId
   * @param call
   * @return connection
   * @throws IOException
   * @throws InterruptedException
   */
  private synchronized Connection getConnection(ConnectionId remoteId, Call call)
      throws IOException, InterruptedException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /*
     * we could avoid this allocation for each RPC by having a connectionsId
     * object and with set() method. We need to manage the refs for keys in
     * HashMap properly. For now its ok
     */
    do {
      connection = connections.get(remoteId);
      if (connection == null) {
        connection = new Connection(remoteId);
        connections.put(remoteId, connection);
      } else if (!connection.channel.isWritable()
          || !connection.channel.isActive()) {
        connection = new Connection(remoteId);
        connections.remove(remoteId);
        connections.put(remoteId, connection);
      }
    } while (!connection.addCall(call));
    // we don't invoke the method below inside "synchronized (connections)"
    // block above. The reason for that is if the server happens to be slow,
    // it will take longer to establish a connection and that will slow the
    // entire system down.

    connection.setupIOstreams();
    return connection;
  }

  /**
   * This class holds the address and the user ticket. The client connections to
   * servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  static class ConnectionId {
    InetSocketAddress address;
    UserGroupInformation ticket;
    Class<?> protocol;
    private static final int PRIME = 16777619;
    private int rpcTimeout;
    private String serverPrincipal;
    private int maxIdleTime; // connections will be culled if it was idle for
                             // maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private int pingInterval; // how often sends ping to the server in msecs

    ConnectionId(InetSocketAddress address, Class<?> protocol,
        UserGroupInformation ticket, int rpcTimeout, String serverPrincipal,
        int maxIdleTime, RetryPolicy connectionRetryPolicy, boolean tcpNoDelay,
        int pingInterval) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.serverPrincipal = serverPrincipal;
      this.maxIdleTime = maxIdleTime;
      this.connectionRetryPolicy = connectionRetryPolicy;
      this.tcpNoDelay = tcpNoDelay;
      this.pingInterval = pingInterval;
    }

    InetSocketAddress getAddress() {
      return address;
    }

    Class<?> getProtocol() {
      return protocol;
    }

    private int getRpcTimeout() {
      return rpcTimeout;
    }

    String getServerPrincipal() {
      return serverPrincipal;
    }

    int getMaxIdleTime() {
      return maxIdleTime;
    }

    boolean getTcpNoDelay() {
      return tcpNoDelay;
    }

    int getPingInterval() {
      return pingInterval;
    }

    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, Configuration conf)
        throws IOException {
      return getConnectionId(addr, protocol, ticket, 0, conf);
    }

    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        Configuration conf) throws IOException {
      return getConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
    }

    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        RetryPolicy connectionRetryPolicy, Configuration conf)
        throws IOException {

      if (connectionRetryPolicy == null) {
        final int max = conf.getInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
        connectionRetryPolicy = RetryPolicies
            .retryUpToMaximumCountWithFixedSleep(max, 1, TimeUnit.SECONDS);
      }

      return new ConnectionId(addr, protocol, ticket, rpcTimeout,
          null,
          conf.getInt("ipc.client.connection.maxidletime", 10000), // 10s
          connectionRetryPolicy,
          conf.getBoolean("ipc.client.tcpnodelay", true),
          AsyncClient.getPingInterval(conf));
    }

    static boolean isEqual(Object a, Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof ConnectionId) {
        ConnectionId that = (ConnectionId) obj;
        return isEqual(this.address, that.address)
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)
            && this.rpcTimeout == that.rpcTimeout
            && isEqual(this.serverPrincipal, that.serverPrincipal)
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = connectionRetryPolicy.hashCode();
      result = PRIME * result + ((address == null) ? 0 : address.hashCode());
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result
          + ((serverPrincipal == null) ? 0 : serverPrincipal.hashCode());
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
  }
}
