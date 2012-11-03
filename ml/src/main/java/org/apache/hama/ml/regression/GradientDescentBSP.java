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
package org.apache.hama.ml.regression;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;
import org.apache.hama.ml.writable.VectorWritable;
import org.apache.hama.util.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A gradient descent (see <code>http://en.wikipedia.org/wiki/Gradient_descent</code>) BSP based implementation.
 */
public class GradientDescentBSP extends BSP<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> {

  private static final Logger log = LoggerFactory.getLogger(GradientDescentBSP.class);
  public static final String INITIAL_THETA_VALUES = "gd.initial.theta";
  public static final String ALPHA = "gd.alpha";
  public static final String COST_THRESHOLD = "gd.cost.threshold";
  public static final String ITERATIONS_THRESHOLD = "gd.iterations.threshold";
  public static final String REGRESSION_MODEL_CLASS = "gd.regression.model";

  private boolean master;
  private DoubleVector theta;
  private double cost;
  private double costThreshold;
  private float alpha;
  private RegressionModel regressionModel;
  private int iterationsThreshold;
  private int m;

  @Override
  public void setup(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException, SyncException, InterruptedException {
    master = peer.getPeerIndex() == peer.getNumPeers() / 2;
    cost = Integer.MAX_VALUE;
    costThreshold = peer.getConfiguration().getFloat(COST_THRESHOLD, 0.1f);
    iterationsThreshold = peer.getConfiguration().getInt(ITERATIONS_THRESHOLD, 10000);
    alpha = peer.getConfiguration().getFloat(ALPHA, 0.003f);
    try {
      regressionModel = ((Class<? extends RegressionModel>) peer.getConfiguration().getClass(REGRESSION_MODEL_CLASS, LinearRegressionModel.class)).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void bsp(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException, SyncException, InterruptedException {
    // 0 superstep : count items

    int itemCount = 0;
    while (peer.readNext() != null) {
      // increment counter
      itemCount++;
    }
    for (String peerName : peer.getAllPeerNames()) {
      if (!peerName.equals(peer.getPeerName())) { // avoid sending to oneself
        peer.send(peerName, new VectorWritable(new DenseDoubleVector(new double[]{itemCount})));
      }
    }
    peer.sync();

    // aggregate number of items
    VectorWritable itemsResult;
    while ((itemsResult = peer.getCurrentMessage()) != null) {
      itemCount += itemsResult.getVector().get(0);
    }

    m = itemCount;

    peer.reopenInput();

    int iterations = 0;
    while (true) {

      getTheta(peer);

      // first superstep : calculate cost function in parallel

      double localCost = 0d;

      // read an item
      KeyValuePair<VectorWritable, DoubleWritable> kvp;
      while ((kvp = peer.readNext()) != null) {
        // calculate cost for given input
        double y = kvp.getValue().get();
        DoubleVector x = kvp.getKey().getVector();
        double costForX = regressionModel.calculateCostForItem(x, y, m, theta);

        // adds to local cost
        localCost += costForX;
      }

      // cost is sent and aggregated by each
      for (String peerName : peer.getAllPeerNames()) {
        if (!peerName.equals(peer.getPeerName())) { // avoid sending to oneself
          peer.send(peerName, new VectorWritable(new DenseDoubleVector(new double[]{localCost})));
        }
      }
      peer.sync();

      // second superstep : aggregate cost calculation
      double totalCost = localCost;
      VectorWritable costResult;
      while ((costResult = peer.getCurrentMessage()) != null) {
        totalCost += costResult.getVector().get(0);
      }

      // cost check
      if (cost - totalCost < 0) {
        throw new RuntimeException(new StringBuilder("gradient descent failed to converge with alpha ").
          append(alpha).toString());
      } else if (totalCost == 0 || totalCost < costThreshold || iterations >= iterationsThreshold) {
        cost = totalCost;
        break;
      } else {
        cost = totalCost;
        if (log.isDebugEnabled()) {
          log.debug(peer.getPeerName() + ": cost is " + cost);
        }
      }

      peer.reopenInput();
      peer.sync();

      double[] thetaDelta = new double[theta.getLength()];

      // third superstep : calculate partial derivatives' deltas in parallel
      while ((kvp = peer.readNext()) != null) {
        DoubleVector x = kvp.getKey().getVector();
        double y = kvp.getValue().get();
        double difference = regressionModel.applyHypothesis(theta, x) - y;
        for (int j = 0; j < theta.getLength(); j++) {
          thetaDelta[j] += difference * x.get(j);
        }
      }

      // send thetaDelta to the each peer
      for (String peerName : peer.getAllPeerNames()) {
        if (!peerName.equals(peer.getPeerName())) { // avoid sending to oneself
          peer.send(peerName, new VectorWritable(new DenseDoubleVector(thetaDelta)));
        }
      }

      peer.sync();

      // fourth superstep : aggregate partial derivatives
      VectorWritable thetaDeltaSlice;
      double[] newTheta = thetaDelta;
      while ((thetaDeltaSlice = peer.getCurrentMessage()) != null) {

        for (int j = 0; j < theta.getLength(); j++) {
          newTheta[j] += thetaDeltaSlice.getVector().get(j);
        }

        for (int j = 0; j < theta.getLength(); j++) {
          newTheta[j] = theta.get(j) - newTheta[j] * alpha;
        }
      }
      theta = new DenseDoubleVector(newTheta);

      if (log.isDebugEnabled()) {
        log.debug(new StringBuilder(peer.getPeerName()).append(": new theta for cost ").
          append(cost).append(" is ").append(theta.toString()).toString());
      }
      // master writes down the output
      if (master) {
        peer.write(new VectorWritable(theta), new DoubleWritable(cost));
      }

      peer.reopenInput();
      peer.sync();

      iterations++;
    }
  }

  @Override
  public void cleanup(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException {
    // master writes down the final output
    if (master) {
      peer.write(new VectorWritable(theta), new DoubleWritable(cost));
      if (log.isInfoEnabled()) {
        log.info(new StringBuilder(peer.getPeerName()).append(":computation finished with cost ").
          append(cost).append(" for theta ").append(theta).toString());
      }
    }
  }

  public void getTheta(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException, SyncException, InterruptedException {
    if (theta == null) {
      if (master) {
        int size = getXSize(peer);
        theta = new DenseDoubleVector(size, peer.getConfiguration().getInt(INITIAL_THETA_VALUES, 1));
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, new VectorWritable(theta));
        }
        if (log.isDebugEnabled()) {
          log.debug(new StringBuilder(peer.getPeerName()).append(": sending theta").toString());
        }
        peer.sync();
      } else {
        if (log.isDebugEnabled()) {
          log.debug(new StringBuilder(peer.getPeerName()).append(": getting theta").toString());
        }
        peer.sync();
        VectorWritable vectorWritable = peer.getCurrentMessage();
        theta = vectorWritable.getVector();
      }
    }
  }

  private int getXSize(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException {
    VectorWritable key = new VectorWritable();
    DoubleWritable value = new DoubleWritable();
    peer.readNext(key, value);
    peer.reopenInput(); // reset input to start
    if (key.getVector() == null) {
      throw new IOException("cannot read input vector size");
    }
    return key.getVector().getLength();
  }
}
