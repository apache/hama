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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.ReflectionUtils;

/**
 * A basic implementation which moves metrics data from sources to sinks.
 */
final class DefaultMetricsSystem implements MetricsSystem, Runnable{ 

  public static final Log LOG = LogFactory.getLog(DefaultMetricsSystem.class);

  private final String prefix;
  private final ScheduledExecutorService sched;
  private final AtomicBoolean state = new AtomicBoolean(false);
  private final HamaConfiguration conf;
  private final MetricsConfig config;
  private final MetricsFilter sourceFilter;
  private final long period;
  private final AtomicReference<MetricsSourceAdaptor> sysSource = 
    new AtomicReference<MetricsSourceAdaptor>();
  private final ConcurrentMap<String, MetricsSourceAdaptor> sources = 
    new ConcurrentHashMap<String, MetricsSourceAdaptor>(); 
  private final ConcurrentMap<String, MetricsSinkAdaptor> sinks = 
    new ConcurrentHashMap<String, MetricsSinkAdaptor>(); 
  private final BlockingQueue<List<Pair>> queue;
  public static final String MS_NAME = "MetricsSystem";
  public static final String MS_STATS_NAME = MS_NAME +",sub=Stats";
  public static final String MS_STATS_DESC = "Metrics system metrics";
  

  DefaultMetricsSystem(String prefix, HamaConfiguration conf){
    this.prefix = prefix;
    this.conf = conf;
    this.config = MetricsConfig.create(this.conf);
    this.sourceFilter = this.config.getFilter("default.source.filter");
    this.period = conf.getInt("bsp.metrics.period", 3); 
    this.queue = new LinkedBlockingQueue<List<Pair>>();
    int workers = conf.getInt("bsp.metrics.threads_pool", 1);
    this.sched = Executors.newScheduledThreadPool(workers);
  }

  public final String prefix(){
    return this.prefix;
  }

  public final long period(){
    return this.period;
  } 

  /**  
   * Start MetricsSystem, scheduling with fix rate. The MetricsSystem 
   * will periodically execute fetching metrics from source to sink.
   */
  public void start(){
    if(true == state.get()){
      LOG.warn("MetricsSystem has already been started.");
      return;
    }
    configure();
    state.set(true);
    this.sched.scheduleAtFixedRate((Runnable)this, 3, period(), SECONDS);
  }

  private void configure(){
    configureSinks();
    configureSources();
  }

  private void configureSinks(){
    Set<MetricsConfig.Entry<String, String>> sinkSubset = 
      this.config.subset(prefix(), "sink");
    for(MetricsConfig.Entry<String, String> entry: sinkSubset){
      String key = entry.key();
      String value = entry.value();
      if(LOG.isDebugEnabled()){
        LOG.debug("Configure sinks' key "+key+" value "+value);
      }
      if(key.endsWith("class")) {
        try{
          LOG.info("Sink name to be registered:"+value);
          MetricsSink sink =  
            ReflectionUtils.newInstance(value);
          register(value, "sink registered from properties file.", (MetricsSink)sink);
        }catch(ClassNotFoundException cnfe) {
          LOG.warn("Class "+value+" not found.", cnfe); 
        }   
      } 
    }
  }

  private void configureSources(){
    registerSystemSource();
  }

  private void registerSystemSource(){
    // sysSource not in sources list
    MetricsSource src = MetricsFactory.createSource(SystemMetrics.class, 
      new Class[]{ Map.class, Map.class, HamaConfiguration.class }, 
      new Object[]{ sources, sinks, conf });
    MetricsSourceAdaptor srcAdp = 
      new MetricsSourceAdaptor(prefix(), MS_STATS_NAME, MS_STATS_DESC, src, 
      (period()+1), conf);
    sysSource.set(srcAdp);
    sysSource.get().start();  
  }

  /**
   * Stop MetricsSystem. First stop source, then sinks; evetually shutting down
   * the thread scheduler.
   */
  public void stop(){
    state.set(false);
    stopSources();
    stopSinks();
    sched.shutdown();
  }

  void stopSources(){
    for(MetricsSourceAdaptor adpt: sources.values()){
      adpt.stop();
    }
    sysSource.get().stop();
    sources.clear();
  }

  void stopSinks(){
    for(MetricsSinkAdaptor adpt: sinks.values()){
      adpt.stop();
    }
    sinks.clear();
  }

  /**
   * Register source and starts as MBean service.
   * @param name to be referenced to the source. 
   * @param description describes the purpose.
   * @param source of metrics.
   */
  public void register(String name, String description, MetricsSource source){
    if(sources.containsKey(name)){
      LOG.warn("Source with name `"+name+"' is already registered.");
      return;
    }
    MetricsSourceAdaptor adpt = 
      new MetricsSourceAdaptor(prefix(), name, description, source, period()+1, 
      this.conf); 
    sources.putIfAbsent(name, adpt);  
    adpt.start(); // register as MBean
    LOG.info("Registered source "+name+".");
  }

  /**
   * Register sink with a specified name.
   * @param name to be associated with sink. 
   * @param description describes the purpose.
   * @param sink is used to consumes metrics.
   */
  public void register(String name, String description, MetricsSink sink){
    if(sinks.containsKey(name)){
      LOG.warn("Sink with name `"+name+"' is already registered.");
      return;
    }
    MetricsFilter srcFilter = this.config.getFilter(prefix()+".source.filter");
    MetricsFilter recordFilter = this.config.getFilter(prefix()+".record.filter");
    MetricsFilter metricFilter = this.config.getFilter(prefix()+".metric.filter");
    MetricsSinkAdaptor adpt = 
      new MetricsSinkAdaptor(name, description, sink, this.queue, this.conf, 
      srcFilter, recordFilter, metricFilter);
    sinks.putIfAbsent(name, adpt);  
    adpt.start(); 
    LOG.info("Registered sink "+name+".");
  }

  /**
   * Periodically harvest metrics data.  
   */
  public void run() {
    if(LOG.isDebugEnabled()){ LOG.debug(" Sinks size "+sinks.size()); }
    if(0 < sinks.size()) publishMetrics(sample()); 
  }

  void publishMetrics(final List<Pair> collections){
    for(MetricsSinkAdaptor adpt: sinks.values()){
      if(LOG.isDebugEnabled()){ LOG.debug(" sink adptr name "+adpt.name()); }
      adpt.putMetrics(collections); 
    }
  }

  /**
   * With corresponded source adaptor, a list of MetricsRecord are collected 
   * and stored into a list of pair, which will be consumed later by sinks
   * that has interest.
   */
  private List<Pair> sample(){
    final List<Pair> collector = new ArrayList<Pair>(); 
    for(Map.Entry<String, MetricsSourceAdaptor> entry:  sources.entrySet()){
      if(null == this.sourceFilter || sourceFilter.accepts(entry.getKey())){
        MetricsSourceAdaptor adpt = entry.getValue();
        collector.add(new Pair(adpt.name(), snapshot(adpt)));
      }
    }
    if(LOG.isDebugEnabled()){ 
      LOG.debug(" Source adaptor size "+collector.size());
    }
    return collector;
  }

  /**
   * Source adaptor harvests metrics information into a list of MetricsRecord. 
   */
  private List<MetricsRecord> snapshot(MetricsSourceAdaptor srcAdpt){
    final List<MetricsRecord> collector = new ArrayList<MetricsRecord>(); 
    srcAdpt.getMetrics(collector); 
    return collector;
  }

  static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    }
    catch (Exception e) {
      LOG.error("Error getting localhost name. "+
                "hostname() would return 'localhost'", e);
    }
    return "localhost";
  }

  @Override
  public MetricsSink findSink(String name){
    MetricsSinkAdaptor adpt = sinks.get(name);
    if(null != adpt){
      return adpt.sink(); 
    }
    return null;
  }

  @Override
  public MetricsSource findSource(String name){
    MetricsSourceAdaptor adpt = sources.get(name);
    if(null != adpt){
      return adpt.source();
    }
    return null;
  }
}
