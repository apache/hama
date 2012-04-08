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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math.MathException;

public final class Node{ 

  public static final Log LOG = LogFactory.getLog(Node.class);

  private final String address;
  /**
   * Sliding window that stores inter-arrival time. For instance,
   * T={10, 12, 14, 17, 23, 25}  Tinter-arrival={2, 2, 3, 6, 2}
   */
  private final ArrayDeque<Double> samplingWindow; // fix size ws
  private final int windowSize;
  /* The latest heartbeat */
  private final AtomicLong latestHeartbeat = new AtomicLong(0);

  public Node(String address, int size){
    this.address = address;
    this.windowSize = size;
    this.samplingWindow = new ArrayDeque<Double>(windowSize);
    if(null == this.address) 
      throw new NullPointerException("Address is not provided");
  }

  public String getAddress(){
    return this.address;
  }

  void setLatestHeartbeat(long latestHeartbeat){
    this.latestHeartbeat.set(latestHeartbeat);
  }

  public long getLatestHeartbeat(){
    return this.latestHeartbeat.get();
  }

  public synchronized void reset(){
    this.samplingWindow.clear();
    setLatestHeartbeat(0); 
  }

  /**
   * The size used for storing samples. 
   * @return int value fixed without change over time.
   */
  public int windowSize(){
    return windowSize;
  }

  /**
   * Inter-arrival times data as array.
   * @return Double array format.
   */
  public Double[] samples(){
    return (Double[]) samplingWindow.toArray(
           new Double[samplingWindow.size()]);
  }

  /**
   * Store the latest inter-arrival time to sampling window.
   * The head of the array will be dicarded. Newly received heartbeat
   * is added to the tail of the sliding window.
   * @param heartbeat value is the current timestamp the client .
   */
  public void add(long heartbeat){
    if(null == this.samplingWindow)
      throw new NullPointerException("Sampling windows not exist.");
    synchronized(this.samplingWindow){
      if(0 != getLatestHeartbeat()) {
        if(samplingWindow.size() == this.windowSize){
          samplingWindow.remove();
        }
        samplingWindow.add(new Double(heartbeat-getLatestHeartbeat()));
      }
      setLatestHeartbeat(heartbeat);
    }
  }

  /**
   * Calculate cumulative distribution function value according to 
   * the current timestamp, heartbeat in sampling window, and last heartbeat. 
   * @param timestamp is the current timestamp.
   * @param samples contain inter-arrival time in the sampling window. 
   * @return double value as cdf, which stays between 0.0 and 1.0. 
   */
  public double cdf(long timestamp, Double[] samples){
    double cdf = -1d;
    double mean = mean(samples);
    double variance = variance(samples);
    NormalDistribution cal = new NormalDistributionImpl(mean, variance);
    try{
         cdf = cal.cumulativeProbability(
           ((double)timestamp-(double)getLatestHeartbeat()));
    }catch(MathException me){
       LOG.error("Fail to compute phi value.", me);
    }
    if(LOG.isDebugEnabled()) LOG.debug("Calcuated cdf:"+cdf);
    return cdf;
  }

  /**
   * Ouput phi value.  
   * @param now is the current timestamp.
   * @return phi value, which goes infinity when cdf returns 1, and 
   *         stays -0.0 when cdf is 0. 
   */
  public double phi(long now){ 
    return (-1) * Math.log10(1-cdf(now, this.samples()));
  }

  /**
   * Mean of the samples.
   * @return double value for mean of the samples.
   */
  public double mean(Double[] samples){
    int len = samples.length;
    if(0 >= len) 
      throw new RuntimeException("Samples data does not exist.");
    double sum = 0d;
    for(double sample: samples){
      sum += sample;
    }
    return sum/(double)len;
  }

  /**
   * Standard deviation.
   * @return double value of standard deviation.
   */
  public double variance(Double[] samples){
    int len = samples.length;
    double mean = mean(samples);
    double sumd = 0d;
    for(double sample: samples)  {
      double v =  sample - mean;
      sumd += v*v;
    }
    return sumd/(double)len;
  }

  @Override
  public boolean equals(final Object target){
    if (target == this)
      return true;
    if (null == target)
      return false;
    if (getClass() != target.getClass())
      return false;

    Node n = (Node) target;
    if(!getAddress().equals(n.address))
      return false;

    return true;
  }

  @Override
  public int hashCode(){
    int result = 17;
    result = 37 * result + address.hashCode();
    return result;
  }

  @Override
  public String toString(){
    Double[] samples = samples();
    StringBuilder builder = new StringBuilder();
    for(double d: samples){
      builder.append(" "+d+" ");
    }
    return "Node address:"+this.address+" mean:"+mean(samples)+" variance:"+
           variance(samples)+" samples:["+builder.toString()+"]";
  }
}
