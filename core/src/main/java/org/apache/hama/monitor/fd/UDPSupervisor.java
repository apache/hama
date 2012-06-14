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
package org.apache.hama.monitor.fd;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hama.monitor.fd.NodeStatus.Dead;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.hama.HamaConfiguration;

/**
 * UDP supervisor is responsible for receiving the heartbeat and output
 * suspicion level for Interpreter.
 */
public class UDPSupervisor implements Supervisor, Callable<Object> {

  public static final Log LOG = LogFactory.getLog(UDPSupervisor.class);

  private static final AtomicInteger WINDOW_SIZE = new AtomicInteger(100);
  private final List<Node> nodes = new CopyOnWriteArrayList<Node>();
  private final ExecutorService receiver;
  private final ExecutorService supervisor;
  private final ScheduledExecutorService watcher;
  private final DatagramChannel channel;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final List<NodeEventListener> listeners = new CopyOnWriteArrayList<NodeEventListener>();

  private final class Pair {

    /**
     * Sliding window that stores inter-arrival time. For instance, T={10, 12,
     * 14, 17, 23, 25} T<inter-arrival>={2, 2, 3, 6, 2}
     */
    ArrayDeque<Double> samplingWindow;

    /** The latest heartbeat */
    long latestHeartbeat;

  }

  /**
   * Each node represents a GroomServer.
   */
  private final class Node {

    /**
     * Host name that represents the node.
     */
    final String host;

    final AtomicReference<Pair> pair = new AtomicReference<Pair>(new Pair());

    /** Window for screening the samples. */
    final int windowSize;

    Node(final String host, final int size) {
      this.host = host;
      this.windowSize = size;
      setSamplingWindow(new ArrayDeque<Double>(windowSize()));
    }

    final String getHost() {
      return this.host;
    }

    final void setLatestHeartbeat(long latestHeartbeat) {
      this.pair.get().latestHeartbeat = latestHeartbeat;
    }

    final long getLatestHeartbeat() {
      return this.pair.get().latestHeartbeat;
    }

    final ArrayDeque<Double> getSamplingWindow() {
      return this.pair.get().samplingWindow;
    }

    final void setSamplingWindow(final ArrayDeque<Double> samplingWindow) {
      this.pair.get().samplingWindow = samplingWindow;
    }

    public void reset() {
      getSamplingWindow().clear();
      setLatestHeartbeat(0);
    }

    /**
     * The size used for storing samples.
     * 
     * @return int value fixed without change over time.
     */
    final int windowSize() {
      return windowSize;
    }

    /**
     * Inter-arrival times data as array.
     * 
     * @return Double array format.
     */
    final Double[] samples() {
      return getSamplingWindow()
          .toArray(new Double[getSamplingWindow().size()]);
    }

    /**
     * Store the latest inter-arrival time to sampling window. The head of the
     * array will be dicarded. Newly received heartbeat is added to the tail of
     * the sliding window.
     * 
     * @param heartbeat value is the current timestamp the client .
     */
    public final void add(long heartbeat) {
      if (null == this.pair.get().samplingWindow)
        throw new NullPointerException("Sampling windows not exist.");
      if (0 != getLatestHeartbeat()) {
        if (getSamplingWindow().size() == windowSize()) {
          getSamplingWindow().remove();
        }
        getSamplingWindow().add(new Double(heartbeat - getLatestHeartbeat()));
      }
      setLatestHeartbeat(heartbeat);
    }

    /**
     * Calculate cumulative distribution function value according to the current
     * timestamp, heartbeat in sampling window, and last heartbeat.
     * 
     * @param timestamp is the current timestamp.
     * @param samples contain inter-arrival time in the sampling window.
     * @return double value as cdf, which stays between 0.0 and 1.0.
     */
    final double cdf(long timestamp, Double[] samples) {
      double cdf = -1d;
      final double mean = mean(samples);
      final double variance = variance(samples);
      // N.B.: NormalDistribution commons math v2.0 will cause init Node hanged.
      final NormalDistribution cal = new NormalDistribution(mean, variance);
      final double rt = (double) timestamp - (double) getLatestHeartbeat();
      cdf = cal.cumulativeProbability(rt);
      if (LOG.isDebugEnabled())
        LOG.debug("Calcuated cdf:" + cdf + " END");
      return cdf;
    }

    /**
     * Ouput phi value.
     * 
     * @param now is the current timestamp.
     * @return phi value, which goes infinity when cdf returns 1, and stays -0.0
     *         when cdf is 0.
     */
    public final double phi(final long now) {
      return (-1) * Math.log10(1 - cdf(now, this.samples()));
    }

    /**
     * Mean of the samples.
     * 
     * @return double value for mean of the samples.
     */
    final double mean(final Double[] samples) {
      int len = samples.length;
      if (0 >= len)
        throw new RuntimeException("Samples data does not exist.");
      double sum = 0d;
      for (double sample : samples) {
        sum += sample;
      }
      return sum / len;
    }

    /**
     * Standard deviation.
     * 
     * @return double value of standard deviation.
     */
    final double variance(final Double[] samples) {
      int len = samples.length;
      double mean = mean(samples);
      double sumd = 0d;
      for (double sample : samples) {
        double v = sample - mean;
        sumd += v * v;
      }
      return sumd / len;
    }

    @Override
    public boolean equals(final Object target) {
      if (target == this)
        return true;
      if (null == target)
        return false;
      if (getClass() != target.getClass())
        return false;

      Node n = (Node) target;
      if (!getHost().equals(n.host))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 37 * result + host.hashCode();
      return result;
    }

    @Override
    public String toString() {
      Double[] samples = samples();
      StringBuilder builder = new StringBuilder();
      for (double d : samples) {
        builder.append(" " + d + " ");
      }
      return "Node host:" + this.host + " mean:" + mean(samples) + " variance:"
          + variance(samples) + " samples:[" + builder.toString() + "]";
    }

  }

  /**
   * Calculate phi value according to the given host.
   */
  final class Calculator implements Callable<Boolean> {
    final Log LOG1 = LogFactory.getLog(Calculator.class);
    final Node target;

    Calculator(final Node target) {
      this.target = target;
    }

    @Override
    public Boolean call() throws Exception {
      double phi = target.phi(System.currentTimeMillis());
      if (LOG1.isDebugEnabled()) {
        LOG1.debug(target.getHost() + "'s phi value is " + phi);
      }
      boolean isAlive = Double.isInfinite(phi) ? false : true;
      if (LOG1.isDebugEnabled()) {
        LOG1.debug(target.getHost() + " is alive? " + isAlive);
      }
      return isAlive;
    }
  }

  // TODO: need policy so that client can be notified according to the rules
  // specified.
  /**
   * Scheduled to check a node's status and notify accordingly.
   */
  final class Watcher implements Runnable {
    final Log LOG2 = LogFactory.getLog(Watcher.class);

    final ExecutorService calculator;

    Watcher() {
      this.calculator = Executors.newSingleThreadExecutor();
    }

    @Override
    public void run() {
      try {
        if (!listeners.isEmpty()) {
          for (Node node : nodes) {
            final String host = node.getHost();
            for (NodeEventListener listener : listeners) {
              NodeStatus[] states = listener.interest();
              for (NodeStatus state : states) {
                if (Dead.equals(state)) {
                  Future<Boolean> result = this.calculator
                      .submit(new Calculator(node));
                  Boolean isAlive = result.get();
                  if (!isAlive) {
                    listener.notify(state, host);
                  }
                } /* else if (Alive.equas(state)) { } */
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        LOG2.warn("Calculator thread is interrupted.", ie);
        Thread.currentThread().interrupt();
      } catch (ExecutionException ee) {
        LOG2.warn(ee);
      }
    }
  }

  final class Hermes implements Callable<Object> {
    private final Node node;
    private final long heartbeat;
    @SuppressWarnings("unused")
    private final long sequence;

    /**
     * Logic unit deals with sensors' status. It first check if the packet
     * coming is 1, indicating the arrival of new packet. Then it checkes the
     * node position within the list, i.e., nodes. If -1 returns, a completely
     * refresh packet arrives; therefore adding node info to the nodes list;
     * otherwise reseting the node sampling window and the latest heartbeat
     * value. If the packet comes with the sequence other than 1, meaning its
     * status of continuous sending heartbeat, thus retrieve old node from list
     * and process necessary steps, such as manipulating sample windows and
     * assigning the last heartbeat.
     * 
     * @param host of specific node equipted with sensor.
     * @param sequence number is generated by the sensor.
     * @param heartbeat timestamped by the supervisor.
     */
    public Hermes(final Node tmpNode, final long sequence, final long heartbeat) {
      int pos = nodes.indexOf(tmpNode);
      Node tmp = null;
      if (1L == sequence) {
        if (-1 == pos) {// fresh
          tmp = tmpNode;
          nodes.add(tmp);
        } else {// node crashed then restarted
          tmp = nodes.get(pos);
          tmp.reset();
        }
      } else {
        if (-1 == pos) {
          LOG.warn("Non existing host (" + tmpNode.getHost()
              + ") is sending heartbeat" + " sequence " + sequence + "!!!");
        } else {
          tmp = nodes.get(pos);
        }
      }
      this.node = tmp;
      if (null == this.node)
        throw new NullPointerException("Node is not correctly assigned.");
      this.heartbeat = heartbeat;
      this.sequence = sequence;
    }

    @Override
    public Object call() throws Exception {
      this.node.add(this.heartbeat);
      return null;
    }
  }

  /**
   * UDP Supervisor.
   */
  public UDPSupervisor(HamaConfiguration conf) {
    DatagramChannel ch = null;
    try {
      ch = DatagramChannel.open();
    } catch (IOException ioe) {
      LOG.error("Fail to open udp channel.", ioe);
    }
    this.channel = ch;
    if (null == this.channel)
      throw new NullPointerException("Channel can not be opened.");
    try {
      this.channel.socket().bind(
          new InetSocketAddress(conf.getInt("bsp.monitor.fd.udp_port", 16384)));
    } catch (SocketException se) {
      LOG.error("Unable to bind the udp socket.", se);
    }
    WINDOW_SIZE.set(conf.getInt("bsp.monitor.fd.window_size", 100));
    this.receiver = Executors.newCachedThreadPool();
    this.supervisor = Executors.newSingleThreadExecutor();
    this.watcher = Executors.newSingleThreadScheduledExecutor();
  }

  /**
   * Register a listener and get notified if a node fails.
   */
  @Override
  public void register(NodeEventListener listener) {
    this.listeners.add(listener);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node event listener " + listener.name() + " is registered.");
    }
  }

  @Override
  public Object call() throws Exception {
    ByteBuffer packet = ByteBuffer.allocate(8);
    try {
      while (running.get()) {
        final InetSocketAddress source = (InetSocketAddress) channel
            .receive(packet);
        final String hostName = source.getHostName();
        packet.flip();
        final long seq = packet.getLong();
        packet.clear();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Seqence: " + seq + " src host: " + hostName);
        }
        final Node tmpNode = new Node(hostName, WINDOW_SIZE.get());
        receiver.submit(new Hermes(tmpNode, seq, System.currentTimeMillis()));
      }
    } catch (IOException ioe) {
      LOG.error("Problem in receiving packet from channel.", ioe);
      Thread.currentThread().interrupt();
    } finally {
      if (null != this.channel)
        try {
          this.channel.socket().close();
          this.channel.close();
        } catch (IOException ioe) {
          LOG.error("Error closing supervisor channel.", ioe);
        }
    }
    return null;
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Supervisor is already started.");
    }
    this.supervisor.submit(this);
    this.watcher.scheduleAtFixedRate(new Watcher(), 0, 1, SECONDS);
  }

  @Override
  public void stop() {
    running.set(false);
    this.watcher.shutdown();
    this.receiver.shutdown();
    this.supervisor.shutdown();
  }

  public boolean isShutdown() {
    return this.channel.socket().isClosed() && !running.get();
  }
}
