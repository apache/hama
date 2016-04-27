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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * An abstract IPC service using netty. IPC calls take a single {@link Writable}
 * as a parameter, and return a {@link Writable}*
 * 
 * @see AsyncServer
 */
public abstract class AsyncServer {

  private AuthMethod authMethod;
  static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  static int INITIAL_RESP_BUF_SIZE = 1024;
  UserGroupInformation user = null;
  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : Introduce the protocol into the RPC connection header
  // 4 : Introduced SASL security layer
  static final byte CURRENT_VERSION = 4;
  static final int HEADER_LENGTH = 10;
  // follows version is read
  private Configuration conf;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  private int backlogLength;;
  InetSocketAddress address;
  private static final Log LOG = LogFactory.getLog(AsyncServer.class);
  private static int NIO_BUFFER_LIMIT = 8 * 1024;
  private final int maxRespSize;
  static final String IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY = "ipc.server.max.response.size";
  static final int IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 1024 * 1024;

  private static final ThreadLocal<AsyncServer> SERVER = new ThreadLocal<AsyncServer>();
  private int port; // port we listen on
  private Class<? extends Writable> paramClass; // class of call parameters
  // Configure the server.(constructor is thread num)
  private EventLoopGroup bossGroup = new EpollEventLoopGroup(1);
  private EventLoopGroup workerGroup = new EpollEventLoopGroup();
  private static final Map<String, Class<?>> PROTOCOL_CACHE = new ConcurrentHashMap<String, Class<?>>();
  private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();

  static Class<?> getProtocolClass(String protocolName, Configuration conf)
      throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }

  /**
   * Getting address
   * 
   * @return InetSocketAddress
   */
  public InetSocketAddress getAddress() {
    return address;
  }

  /**
   * Returns the server instance called under or null. May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values. Permits applications to access the
   * server context.
   * 
   * @return NioServer
   */
  public static AsyncServer get() {
    return SERVER.get();
  }

  /**
   * Constructs a server listening on the named port and address. Parameters
   * passed must be of the named class. The
   * <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * 
   * @param bindAddress
   * @param port
   * @param paramClass
   * @param handlerCount
   * @param conf
   * @throws IOException
   */
  protected AsyncServer(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount, Configuration conf)
      throws IOException {
    this(bindAddress, port, paramClass, handlerCount, conf, Integer
        .toString(port), null);
  }

  protected AsyncServer(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount,
      Configuration conf, String serverName) throws IOException {
    this(bindAddress, port, paramClass, handlerCount, conf, serverName, null);
  }

  protected AsyncServer(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount,
      Configuration conf, String serverName,
      SecretManager<? extends TokenIdentifier> secretManager)
      throws IOException {
    this.conf = conf;
    this.port = port;
    this.address = new InetSocketAddress(bindAddress, port);
    this.paramClass = paramClass;
    this.maxRespSize = conf.getInt(IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);

    this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", true);
    this.backlogLength = conf.getInt("ipc.server.listen.queue.size", 100);
  }

  /** start server listener */
  public void start() throws ExecutionException, InterruptedException {
    ExecutorService es = Executors.newSingleThreadExecutor();
    Future<ChannelFuture> future = es.submit(new NioServerListener());
    try {
      ChannelFuture closeFuture = future.get();
      closeFuture
          .addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
            @Override
            public void operationComplete(
                io.netty.util.concurrent.Future<Void> voidFuture)
                throws Exception {
              // Stop the server gracefully if it's not terminated.
              stop();
            }
          });
    } finally {
      es.shutdown();
    }
  }

  private class NioServerListener implements Callable<ChannelFuture> {

    /**
     * Configure and start nio server
     */
    @Override
    public ChannelFuture call() throws Exception {
      SERVER.set(AsyncServer.this);
      // ServerBootstrap is a helper class that sets up a server
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(EpollServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, backlogLength)
          .childOption(ChannelOption.MAX_MESSAGES_PER_READ, NIO_BUFFER_LIMIT)
          .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
          .childOption(ChannelOption.SO_KEEPALIVE, true)
          .childOption(ChannelOption.SO_RCVBUF, 30 * 1024 * 1024)
          .childOption(ChannelOption.RCVBUF_ALLOCATOR,
              new FixedRecvByteBufAllocator(100 * 1024))

          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline p = ch.pipeline();
              // Register accumulation processing handler
              p.addLast(new NioFrameDecoder(100 * 1024 * 1024, 0, 4, 0, 0));
              // Register message processing handler
              p.addLast(new NioServerInboundHandler());
            }
          });

      // Bind and start to accept incoming connections.
      ChannelFuture f = b.bind(port).sync();
      LOG.info("AsyncServer startup");

      return f.channel().closeFuture();
    }
  }

  /** Stops the server gracefully. */
  public void stop() {
    if (bossGroup != null && !bossGroup.isTerminated()) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null && !workerGroup.isTerminated()) {
      workerGroup.shutdownGracefully();
    }
    LOG.info("AsyncServer gracefully shutdown");
  }

  /**
   * This class dynamically accumulate the recieved data by the value of the
   * length field in the message
   */
  public class NioFrameDecoder extends LengthFieldBasedFrameDecoder {

    /**
     * @param maxFrameLength - the maximum length of the frame
     * @param lengthFieldOffset - the offset of the length field
     * @param lengthFieldLength - the length of the length field
     * @param lengthAdjustment - the compensation value to add to the value of
     *          the length field
     * @param initialBytesToStrip - the number of first bytes to strip out from
     *          the decoded frame
     */
    public NioFrameDecoder(int maxFrameLength, int lengthFieldOffset,
        int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip) {
      super(maxFrameLength, lengthFieldOffset, lengthFieldLength,
          lengthAdjustment, initialBytesToStrip);
    }

    /**
     * Decode(Accumulate) the from one ByteBuf to an other
     * 
     * @param ctx
     * @param in
     */
    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
        throws Exception {
      ByteBuf recvBuff = (ByteBuf) super.decode(ctx, in);
      if (recvBuff == null) {
        return null;
      }
      return recvBuff;
    }
  }

  /**
   * This class process received message from client and send response message.
   */
  private class NioServerInboundHandler extends ChannelInboundHandlerAdapter {
    ConnectionHeader header = new ConnectionHeader();
    Class<?> protocol;
    private String errorClass = null;
    private String error = null;
    private boolean rpcHeaderRead = false; // if initial rpc header is read
    private boolean headerRead = false; // if the connection header that follows
                                        // version is read.

    /**
     * Be invoked only one when a connection is established and ready to
     * generate traffic
     * 
     * @param ctx
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      SERVER.set(AsyncServer.this);
    }

    /**
     * Process a recieved message from client. This method is called with the
     * received message, whenever new data is received from a client.
     * 
     * @param ctx
     * @param cause
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuffer dataLengthBuffer = ByteBuffer.allocate(4);
      ByteBuf byteBuf = (ByteBuf) msg;

      ByteBuffer data = null;
      ByteBuffer rpcHeaderBuffer = null;
      try {
        while (true) {
          Call call = null;
          errorClass = null;
          error = null;
          try {
            if (dataLengthBuffer.remaining() > 0 && byteBuf.isReadable()) {
              byteBuf.readBytes(dataLengthBuffer);
              if (dataLengthBuffer.remaining() > 0 && byteBuf.isReadable()) {
                return;
              }
            } else {
              return;
            }

            // read rpcHeader
            if (!rpcHeaderRead) {
              // Every connection is expected to send the header.
              if (rpcHeaderBuffer == null) {
                dataLengthBuffer = null;
                dataLengthBuffer = ByteBuffer.allocate(4);
                byteBuf.readBytes(dataLengthBuffer);
                rpcHeaderBuffer = ByteBuffer.allocate(2);
              }
              byteBuf.readBytes(rpcHeaderBuffer);
              if (!rpcHeaderBuffer.hasArray()
                  || rpcHeaderBuffer.remaining() > 0) {
                return;
              }
              int version = rpcHeaderBuffer.get(0);
              byte[] method = new byte[] { rpcHeaderBuffer.get(1) };
              try {
                authMethod = AuthMethod.read(new DataInputStream(
                    new ByteArrayInputStream(method)));
                dataLengthBuffer.flip();
              } catch (IOException ioe) {
                errorClass = ioe.getClass().getName();
                error = StringUtils.stringifyException(ioe);
              }

              if (!HEADER.equals(dataLengthBuffer)
                  || version != CURRENT_VERSION) {
                LOG.warn("Incorrect header or version mismatch from "
                    + address.getHostName() + ":" + address.getPort()
                    + " got version " + version + " expected version "
                    + CURRENT_VERSION);
                return;
              }
              dataLengthBuffer.clear();
              if (authMethod == null) {
                throw new RuntimeException(
                    "Unable to read authentication method");
              }
              rpcHeaderBuffer = null;
              rpcHeaderRead = true;
              continue;
            }

            // read data length and allocate buffer;
            if (data == null) {
              dataLengthBuffer.flip();
              int dataLength = dataLengthBuffer.getInt();
              if (dataLength < 0) {
                LOG.warn("Unexpected data length " + dataLength + "!! from "
                    + address.getHostName());
              }
              data = ByteBuffer.allocate(dataLength);
            }

            // read received data
            byteBuf.readBytes(data);
            if (data.remaining() == 0) {
              dataLengthBuffer.clear();
              data.flip();
              boolean isHeaderRead = headerRead;
              call = processOneRpc(data.array());
              data = null;
              if (!isHeaderRead) {
                continue;
              }
            }
          } catch (OutOfMemoryError oome) {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn("Out of Memory in server select", oome);
            try {
              Thread.sleep(60000);
              errorClass = oome.getClass().getName();
              error = StringUtils.stringifyException(oome);
            } catch (Exception ie) {
            }
          } catch (Exception e) {
            LOG.warn("Exception in Responder "
                + StringUtils.stringifyException(e));
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          }
          sendResponse(ctx, call);
        }
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }

    /**
     * Send response data to client
     * 
     * @param ctx
     * @param call
     */
    private void sendResponse(ChannelHandlerContext ctx, Call call) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream(
          INITIAL_RESP_BUF_SIZE);
      Writable value = null;
      try {
        value = call(protocol, call.param, call.timestamp);
      } catch (Throwable e) {
        String logMsg = this.getClass().getName() + ", call " + call
            + ": error: " + e;
        if (e instanceof RuntimeException || e instanceof Error) {
          // These exception types indicate something is probably wrong
          // on the server side, as opposed to just a normal exceptional
          // result.
          LOG.warn(logMsg, e);
        } else if (exceptionsHandler.isTerse(e.getClass())) {
          // Don't log the whole stack trace of these exceptions.
          // Way too noisy!
          LOG.info(logMsg);
        } else {
          LOG.info(logMsg, e);
        }
        errorClass = e.getClass().getName();
        error = StringUtils.stringifyException(e);
      }
      try {
        setupResponse(buf, call, (error == null) ? Status.SUCCESS
            : Status.ERROR, value, errorClass, error);
        if (buf.size() > maxRespSize) {
          LOG.warn("Large response size " + buf.size() + " for call "
              + call.toString());
          buf = new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
        }
        // send response data;
        channelWrite(ctx, call.response);
      } catch (Exception e) {
        LOG.info(this.getClass().getName() + " caught: "
            + StringUtils.stringifyException(e));
        error = null;
      } finally {
        IOUtils.closeStream(buf);
      }
    }

    /**
     * read header or data
     * 
     * @param buf
     * @return
     */
    private Call processOneRpc(byte[] buf) throws IOException {
      if (headerRead) {
        return processData(buf);
      } else {
        processHeader(buf);
        headerRead = true;
        return null;
      }
    }

    /**
     * Reads the connection header following version
     * 
     * @param buf buffer
     */
    private void processHeader(byte[] buf) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
      try {
        header.readFields(in);
        String protocolClassName = header.getProtocol();
        if (protocolClassName != null) {
          protocol = getProtocolClass(header.getProtocol(), conf);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        IOUtils.closeStream(in);
      }

      UserGroupInformation protocolUser = header.getUgi();
      user = protocolUser;
    }

    /**
     * 
     * Reads the received data, create call object;
     * 
     * @param buf buffer to serialize the response into
     * @return the IPC Call
     */
    private Call processData(byte[] buf) {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
      try {
        int id = dis.readInt(); // try to read an id

        if (LOG.isDebugEnabled())
          LOG.debug(" got #" + id);
        Writable param = ReflectionUtils.newInstance(paramClass, conf);
        param.readFields(dis); // try to read param data

        Call call = new Call(id, param, this);

        return call;
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        IOUtils.closeStream(dis);
      }
    }
  }

  /**
   * Setup response for the IPC Call.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status {@link Status} of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream response, Call call,
      Status status, Writable rv, String errorClass, String error)
      throws IOException {
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.id); // write call id
    out.writeInt(status.state); // write status

    if (status == Status.SUCCESS) {
      rv.write(out);
    } else {
      WritableUtils.writeString(out, errorClass);
      WritableUtils.writeString(out, error);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
    IOUtils.closeStream(out);
  }

  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}. If
   * the amount of data is large, it writes to channel in smaller chunks. This
   * is to avoid jdk from creating many direct buffers as the size of buffer
   * increases. This also minimizes extra copies in NIO layer as a result of
   * multiple write operations required to write a large buffer.
   * 
   * @see WritableByteChannel#write(ByteBuffer)
   * 
   * @param ctx
   * @param buffer
   */
  private void channelWrite(ChannelHandlerContext ctx, ByteBuffer buffer) {
    try {
      ByteBuf buf = ctx.alloc().buffer();
      buf.writeBytes(buffer.array());
      ctx.writeAndFlush(buf);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  /** A call queued for handling. */
  private static class Call {
    private int id; // the client's call id
    private Writable param; // the parameter passed
    private ChannelInboundHandlerAdapter connection; // connection to client
    private long timestamp; // the time received when response is null
    // the time served when response is not null
    private ByteBuffer response; // the response for this call

    /**
     * 
     * @param id
     * @param param
     * @param connection
     */
    public Call(int id, Writable param, ChannelInboundHandlerAdapter connection) {
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
    }

    /**
     * 
     */
    @Override
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }

    /**
     * 
     * @param response
     */
    public void setResponse(ByteBuffer response) {
      this.response = response;
    }
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling e.g., terse
   * exception group for concise logging messages
   */
  static class ExceptionsHandler {
    private volatile Set<String> terseExceptions = new HashSet<String>();

    /**
     * Add exception class so server won't log its stack trace. Modifying the
     * terseException through this method is thread safe.
     * 
     * @param exceptionClass exception classes
     */
    void addTerseExceptions(Class<?>... exceptionClass) {

      // Make a copy of terseException for performing modification
      final HashSet<String> newSet = new HashSet<String>(terseExceptions);

      // Add all class names into the HashSet
      for (Class<?> name : exceptionClass) {
        newSet.add(name.toString());
      }
      // Replace terseException set
      terseExceptions = Collections.unmodifiableSet(newSet);
    }

    /**
     * 
     * @param t
     * @return
     */
    boolean isTerse(Class<?> t) {
      return terseExceptions.contains(t.toString());
    }
  }

  /**
   * Called for each call.
   * 
   * @param protocol
   * @param param
   * @param receiveTime
   * @return Writable
   * @throws IOException
   */
  public abstract Writable call(Class<?> protocol, Writable param,
      long receiveTime) throws IOException;
}
