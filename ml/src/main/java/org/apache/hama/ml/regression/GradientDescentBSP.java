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
import java.util.Arrays;

/**
 * A gradient descent (see
 * <code>http://en.wikipedia.org/wiki/Gradient_descent</code>) BSP based
 * implementation.
 */
public class GradientDescentBSP
    extends
    BSP<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> {

  private static final Logger log = LoggerFactory
      .getLogger(GradientDescentBSP.class);
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

  @SuppressWarnings("unchecked")
  @Override
  public void setup(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException, SyncException, InterruptedException {
    master = peer.getPeerIndex() == peer.getNumPeers() / 2;
    cost = Double.MAX_VALUE;
    costThreshold = peer.getConfiguration().getFloat(COST_THRESHOLD, 0.1f);
    iterationsThreshold = peer.getConfiguration().getInt(ITERATIONS_THRESHOLD,
        10000);
    alpha = peer.getConfiguration().getFloat(ALPHA, 0.003f);
    try {
      regressionModel = ((Class<? extends RegressionModel>) peer
          .getConfiguration().getClass(REGRESSION_MODEL_CLASS,
              LinearRegressionModel.class)).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void bsp(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException, SyncException, InterruptedException {
    // 0a superstep: get initial theta
    getInitialTheta(peer);

    // 0b superstep: count items
    int itemCount = 0;
    while (peer.readNext() != null) {
      // increment counter
      itemCount++;
    }
    broadcastVector(peer, new double[] { itemCount });
    peer.sync();

    // aggregate number of items
    aggregateItemsNumber(peer, itemCount);

    peer.reopenInput();

    int iterations = 0;
    while (true) {

      // first superstep : calculate cost function in parallel
      double localCost = calculateLocalCost(peer);

      // cost is sent and aggregated by each
      broadcastVector(peer, new double[] { localCost });
      peer.sync();

      // second superstep : aggregate cost calculation
      double totalCost = aggregateTotalCost(peer, localCost);

      // cost check
      if (checkCost(peer, iterations, totalCost))
        break;

      peer.sync();
      peer.reopenInput();

      // third superstep : calculate partial derivatives' deltas in parallel
      double[] thetaDelta = calculatePartialDerivatives(peer);

      // send thetaDelta to the each peer
      broadcastVector(peer, thetaDelta);

      peer.sync();

      // fourth superstep : aggregate partial derivatives
      double[] newTheta = aggregatePartialDerivatives(peer, thetaDelta);

      // update theta
      updateTheta(newTheta);

      if (log.isDebugEnabled()) {
        log.debug(new StringBuilder(peer.getPeerName())
            .append(": new theta for cost ").append(cost).append(" is ")
            .append(theta.toString()).toString());
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

  private double aggregateTotalCost(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer,
      double localCost) throws IOException {
    double totalCost = localCost;
    VectorWritable costResult;
    while ((costResult = peer.getCurrentMessage()) != null) {
      totalCost += costResult.getVector().get(0);
    }
    return totalCost;
  }

  private double[] aggregatePartialDerivatives(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer,
      double[] thetaDelta) throws IOException {
    VectorWritable thetaDeltaSlice;
    double[] newTheta = Arrays.copyOf(thetaDelta, thetaDelta.length);
    while ((thetaDeltaSlice = peer.getCurrentMessage()) != null) {
      for (int j = 0; j < theta.getLength(); j++) {
        newTheta[j] += thetaDeltaSlice.getVector().get(j);
      }
    }
    return newTheta;
  }

  private void updateTheta(double[] thetaDiff) {
    double[] newTheta = new double[theta.getLength()];
    for (int j = 0; j < theta.getLength(); j++) {
      newTheta[j] = theta.get(j) - thetaDiff[j] * alpha;
    }
    theta = new DenseDoubleVector(newTheta);
  }

  private void aggregateItemsNumber(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer,
      int itemCount) throws IOException {
    VectorWritable itemsResult;
    while ((itemsResult = peer.getCurrentMessage()) != null) {
      itemCount += itemsResult.getVector().get(0);
    }

    m = itemCount;
  }

  private boolean checkCost(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer,
      int iterations, double totalCost) {
    if (iterations > 0 && cost < totalCost) {
      throw new RuntimeException(new StringBuilder(
          "gradient descent failed to converge with alpha ").append(alpha)
          .toString());
    } else if (totalCost == 0 || totalCost < costThreshold
        || iterations >= iterationsThreshold) {
      cost = totalCost;
      return true;
    } else {
      cost = totalCost;
      if (log.isDebugEnabled()) {
        log.debug(new StringBuilder(peer.getPeerName())
            .append(": current cost is ").append(cost).toString());
      }
      return false;
    }
  }

  private double calculateLocalCost(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException {
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
    return localCost;
  }

  private void broadcastVector(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer,
      double[] vector) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      if (!peerName.equals(peer.getPeerName())) { // avoid sending to oneself
        peer.send(peerName, new VectorWritable(new DenseDoubleVector(vector)));
      }
    }
  }

  private double[] calculatePartialDerivatives(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException {
    KeyValuePair<VectorWritable, DoubleWritable> kvp;
    double[] thetaDelta = new double[theta.getLength()];
    while ((kvp = peer.readNext()) != null) {
      DoubleVector x = kvp.getKey().getVector();
      double y = kvp.getValue().get();
      double difference = regressionModel.applyHypothesis(theta, x) - y;
      for (int j = 0; j < theta.getLength(); j++) {
        thetaDelta[j] += difference * x.get(j);
      }
    }
    return thetaDelta;
  }

  @Override
  public void cleanup(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException {
    // master writes down the final output
    if (master) {
      peer.write(new VectorWritable(theta), new DoubleWritable(cost));
      if (log.isInfoEnabled()) {
        log.info(new StringBuilder(peer.getPeerName())
            .append(":computation finished with cost ").append(cost)
            .append(" for theta ").append(theta).toString());
      }
    }
  }

  public void getInitialTheta(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException, SyncException, InterruptedException {
    if (theta == null) {
      if (master) {
        int size = getXSize(peer);
        theta = new DenseDoubleVector(size, peer.getConfiguration().getInt(
            INITIAL_THETA_VALUES, 1));
        broadcastVector(peer, theta.toArray());
        if (log.isDebugEnabled()) {
          log.debug(new StringBuilder(peer.getPeerName()).append(
              ": sending theta").toString());
        }
        peer.sync();
      } else {
        if (log.isDebugEnabled()) {
          log.debug(new StringBuilder(peer.getPeerName()).append(
              ": getting theta").toString());
        }
        peer.sync();
        VectorWritable vectorWritable = peer.getCurrentMessage();
        theta = vectorWritable.getVector();
      }
    }
  }

  private int getXSize(
      BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer)
      throws IOException {
    VectorWritable key = new VectorWritable();
    DoubleWritable value = new DoubleWritable();
    peer.readNext(key, value);
    peer.reopenInput(); // reset input to start
    if (key.getVector() == null) {
      throw new IOException("cannot read input vector size");
    }
    return key.getVector().getDimension();
  }
}
