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
package org.apache.hama.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.List;
import java.util.Queue;
import static java.util.concurrent.TimeUnit.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

class MetricsSinkAdaptor {

  public static final Log LOG = LogFactory.getLog(MetricsSinkAdaptor.class);

  private final String name, description;
  private final BlockingQueue<List<Pair>> queue; 
  private final HamaConfiguration conf;
  private final int MAX_QUEUE_SIZE;
  private final Worker worker;
  private final MetricsFilter sourceFilter, recordFilter, metricFilter;

  /**
   * Resolve and move pair data to corresponded sink. 
   * Each MetricsSinkAdaptor instance will equipped with one
   * worker consuming metrics data
   */
  private class Worker implements Callable {
    private final ScheduledExecutorService sched;
    private final BlockingQueue<List<Pair>> queue;
    private final MetricsSink sink;
    private final AtomicBoolean state = new AtomicBoolean(false);

    Worker(BlockingQueue<List<Pair>> queue, MetricsSink sink){
      this.queue = queue;
      this.sink = sink;
      this.sched = Executors.newScheduledThreadPool(1);
    } 

    void start(){
      state.set(true);
      sched.schedule(this, 0, SECONDS); 
    }
 
    MetricsSink sink(){
      return this.sink;
    }

    /**
     * Consume metrics with corresponded sink.
     */
    public Object call() throws Exception {
      while(state.get()){
        List<Pair> pairs = this.queue.take();
        if(LOG.isDebugEnabled()){
          LOG.debug(" Pairs (in queue) size "+((null!=pairs)?pairs.size():"0"));
        }
        for(Pair nameAndRecords: pairs){ // adpt.name & metricsRecords
          if(LOG.isDebugEnabled()){
            LOG.debug(" Pair's name "+nameAndRecords.name()+" record size "+
            ((null!=nameAndRecords.records())?nameAndRecords.records().size():"0"));
          }
          if (sourceFilter == null || sourceFilter.accepts(nameAndRecords.name())) {
            for(MetricsRecord record: nameAndRecords.records()){
              if (recordFilter == null || recordFilter.accepts(record)) { // TODO: context
                sink.putMetrics(record);
              }
            }// for
          }
        }
      }
      return null;
    }

    void stop(){
      state.set(false);
      sched.shutdown();
    }
  }

  MetricsSinkAdaptor(String name, String description, MetricsSink sink, 
      BlockingQueue<List<Pair>> queue, HamaConfiguration conf, 
      MetricsFilter sourceFilter, MetricsFilter recordFilter, 
      MetricsFilter metricFilter){
    this.name = name;
    this.description = description;
    this.queue = queue; // contains a list of name and List<MetricsRecords> pair
    this.conf = conf;
    this.MAX_QUEUE_SIZE = conf.getInt("bsp.metrics.max_queue_size", 100);
    worker = new Worker(queue, sink);
    this.sourceFilter = sourceFilter;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
  }

  /**
   * Store metrics data into queue.
   */
  void putMetrics(final List<Pair> pairs){
    try{
      if(queue.size() == this.MAX_QUEUE_SIZE){
        queue.take();
      }
      queue.put(pairs);
    }catch(Exception e){
      LOG.error("Fail to add data to designated queue.", e);
    }
  }

  void start(){
    worker.start(); 
  } 

  void stop(){
    worker.stop();
  }
  
  final String name(){
    return this.name;
  }

  final String description(){
    return this.description;
  }

  public MetricsSink sink(){
    return worker.sink();
  }
}
