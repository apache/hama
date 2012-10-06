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
 * A gradient descent (see <code>http://en.wikipedia.org/wiki/Gradient_descent</code>) BSP based abstract implementation.
 * Each extending class should implement the #applyHypothesis(DoubleVector theta, DoubleVector x) method for a specific
 */
public abstract class GradientDescentBSP extends BSP<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> {

  private static final Logger log = LoggerFactory.getLogger(GradientDescentBSP.class);
  static final String INITIAL_THETA_VALUES = "initial.theta.values";
  static final String ALPHA = "alpha";

  private boolean master;
  private DoubleVector theta;

  @Override
  public void setup(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException, SyncException, InterruptedException {
    master = peer.getPeerIndex() == peer.getNumPeers() / 2;
  }

  @Override
  public void bsp(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException, SyncException, InterruptedException {

    while (true) {

      getTheta(peer);

      // first superstep : calculate cost function in parallel

      double localCost = 0d;

      int numRead = 0;

      // read an input
      KeyValuePair<VectorWritable, DoubleWritable> kvp;
      while ((kvp = peer.readNext()) != null) {

        // calculate cost for given input
        double y = kvp.getValue().get();
        DoubleVector x = kvp.getKey().getVector();
        double costForX = calculateCostForItem(y, x, theta);

        // adds to local cost
        localCost += costForX;
        numRead++;
      }

      // cost is sent and aggregated by each
      double totalCost = localCost;

      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new VectorWritable(new DenseDoubleVector(new double[]{localCost, numRead})));
      }
      peer.sync();

      // second superstep : aggregate cost calculation

      VectorWritable costResult;
      while ((costResult = peer.getCurrentMessage()) != null) {
        totalCost += costResult.getVector().get(0);
        numRead += costResult.getVector().get(1);
      }

      totalCost /= numRead;

      if (log.isInfoEnabled()) {
        log.info("cost is " + totalCost);
      }

      peer.sync();

      peer.reopenInput();

      double[] thetaDelta = new double[theta.getLength()];

      // third superstep : calculate partial derivatives' deltas in parallel
      while ((kvp = peer.readNext()) != null) {
        DoubleVector x = kvp.getKey().getVector();
        double y = kvp.getValue().get();
        double difference = applyHypothesis(theta, x) - y;
        for (int j = 0; j < theta.getLength(); j++) {
          thetaDelta[j] += difference * x.get(j);
        }
      }

      // send thetaDelta to the each peer
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new VectorWritable(new DenseDoubleVector(thetaDelta)));
      }

      peer.sync();

      // fourth superstep : aggregate partial derivatives
      VectorWritable thetaDeltaSlice;
      while ((thetaDeltaSlice = peer.getCurrentMessage()) != null) {
        double[] newTheta = new double[theta.getLength()];

        for (int j = 0; j < theta.getLength(); j++) {
          newTheta[j] += thetaDeltaSlice.getVector().get(j);
        }

        for (int j = 0; j < theta.getLength(); j++) {
          newTheta[j] = theta.get(j) - newTheta[j] * peer.getConfiguration().getFloat(ALPHA, 0.3f);
        }

        theta = new DenseDoubleVector(newTheta);

        if (log.isInfoEnabled()) {
          log.info("new theta for cost " + totalCost + " is " + theta.toArray().toString());
        }
        // master writes down the output
        if (master) {
          peer.write(new VectorWritable(theta), new DoubleWritable(totalCost));
        }
      }
      peer.sync();

      // eventually break execution !?
      if (totalCost == 0) {
        // TODO change this as just 0 is too strict
        break;
      }
    }

  }

  /**
   * Calculates the cost function for a given item (input x, output y)
   * @param y the learned output for x
   * @param x the input vector
   * @param theta the parameters vector theta
   * @return the calculated cost for input x and output y
  */
  protected abstract double calculateCostForItem(double y, DoubleVector x, DoubleVector theta);

  /**
   * Applies the applyHypothesis given a set of parameters theta to a given input x
   *
   * @param theta the parameters vector
   * @param x     the input
   * @return a <code>double</code> number
   */
  public abstract double applyHypothesis(DoubleVector theta, DoubleVector x);


  public void getTheta(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException, SyncException, InterruptedException {
    if (master && theta == null) {
      int size = getXSize(peer);
      theta = new DenseDoubleVector(size, peer.getConfiguration().getInt(INITIAL_THETA_VALUES, 10));
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new VectorWritable(theta));
      }
      peer.sync();
    } else {
      peer.sync();
      VectorWritable vectorWritable = peer.getCurrentMessage();
      theta = vectorWritable.getVector();
    }
  }

  private int getXSize(BSPPeer<VectorWritable, DoubleWritable, VectorWritable, DoubleWritable, VectorWritable> peer) throws IOException {
    VectorWritable key = null;
    peer.readNext(key, null);
    peer.reopenInput(); // reset input to start
    if (key == null) {
      throw new IOException("cannot read input vector size");
    }
    return key.getVector().getLength();
  }
}
