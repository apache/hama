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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

class MetricsSourceAdaptor implements DynamicMBean {

  public static final Log LOG = LogFactory.getLog(MetricsSourceAdaptor.class);
   
  private final String prefix;
  private final AtomicReference<List<MetricsRecord>> lastRecs =  
    new AtomicReference<List<MetricsRecord>>();
  private final AtomicReference<MBeanInfo> infoCache = 
    new AtomicReference<MBeanInfo>();
  private final ConcurrentMap<String, Attribute> attrCache;  
  private final long jmxCacheTTL;
  private final AtomicReference<ObjectName> mbeanName = 
    new AtomicReference<ObjectName>();
  private final AtomicBoolean startMBeans = new AtomicBoolean(false);
  private final MetricsSource source;
  private String name, description;
  
  MetricsSourceAdaptor(final String prefix, final String name, 
      final String description, final MetricsSource source, 
      final long jmxCacheTTL, final HamaConfiguration conf){
    this.prefix = prefix;
    this.attrCache = new ConcurrentHashMap<String, Attribute>();
    this.name = name;
    this.description = description;
    this.source = source;
    this.jmxCacheTTL = jmxCacheTTL;
    startMBeans.set(conf.getBoolean("bsp.metrics.start_mbeans", true));
  }

  void start(){
    if(null != mbeanName.get()) return;
    if(startMBeans.get()) startMBeans();
  }

  void stop(){
    stopMBeans();
  }

  public void stopMBeans(){
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();  
    if(null == mbeanName.get()) {
      LOG.error("MBean object name not found!");
      throw new NullPointerException("MBean object name not found!");
    }
    try {
      mbs.unregisterMBean(mbeanName.get());
    } catch (Exception e) {
      LOG.warn("Error unregistering "+ mbeanName.get(), e);
    }
    mbeanName.set(null);
  }
  
  private void startMBeans(){
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    String mbeanStr = "Hama:service="+ prefix +",name="+ name();//+",sub=Stats";
  //String nameStr =  "Hadoop:service="+ serviceName +",name="+ nameName; -> haddop metrics2
  //                   Hama:service=MetricsSystem,sub=Stats,name=MetricsSystem,sub=Stats,sub=Stats -> error in test
    try{
      this.mbeanName.set(new ObjectName(mbeanStr));
    }catch(Exception e){
      LOG.error("Unable to create MBean Object name "+mbeanStr, e);
    }
    try{
      mbs.registerMBean(this, mbeanName.get());
      LOG.info("Registered MBean "+mbeanName);
    }catch(Exception e){
      LOG.error("Unable to register MBean "+mbeanStr, e);
    }
  }

  @Override
  public synchronized Object getAttribute(String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    updateJmxCache();
    Attribute a = attrCache.get(attribute);
    if (a == null) {
      throw new AttributeNotFoundException(attribute +" not found");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(attribute +": "+ a);
    }
    return a.getValue();
  }

  @Override
  public synchronized AttributeList getAttributes(String[] attributes) {
    updateJmxCache();
    AttributeList ret = new AttributeList();
    for (String key : attributes) {
      Attribute attr = attrCache.get(key);
      if (LOG.isDebugEnabled()) {
        LOG.debug(key +": "+ attr);
      }
      ret.add(attr);
    }
    return ret;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    updateJmxCache();
    return infoCache.get();
  }

  private void updateJmxCache(){
    long jmxCacheTS = 0;
    if (System.currentTimeMillis() - jmxCacheTS >= jmxCacheTTL) {
      if(null == lastRecs.get()){
        final List<MetricsRecord> collector = new ArrayList<MetricsRecord>();
        getMetrics(collector); 
      }
      int oldCacheSize = attrCache.size();
      int newCacheSize = updateAttrCache();
      if(oldCacheSize < newCacheSize){
        updateInfoCache(); 
      }
      jmxCacheTS = System.currentTimeMillis(); 
      lastRecs.set(null);
    }
  }

  private void updateInfoCache(){
    List<MBeanAttributeInfo> attrs = new ArrayList<MBeanAttributeInfo>();
    for(MetricsRecord record: lastRecs.get()){ // tag 
      for(MetricsTag t: record.tags()){
        attrs.add(new MBeanAttributeInfo("tag."+t.name(), "java.lang.String", 
                  t.name()+" record", true, false, false)); // read-only
      }
      for(Metric m: record.metrics()){ // metrics
        attrs.add(new MBeanAttributeInfo(m.name(), m.value().getClass().toString(), 
                  m.name(), true, false, false)); // read-only
      }
    }
    MBeanAttributeInfo[] attrsArray = new MBeanAttributeInfo[attrs.size()];
    infoCache.set(new MBeanInfo(name(), description(),  
                  attrs.toArray(attrsArray), null, null, null));
  }

  public void getMetrics(final List<MetricsRecord> collector){
    this.source.snapshot(collector);
    lastRecs.set(collector);
  }
  
  private int updateAttrCache() {
    int recNo = 0;
    int numMetrics = 0;
    for(MetricsRecord record: lastRecs.get()){
      for(MetricsTag t:  record.tags()){
        setAttrCacheTag(t, recNo);  
        ++numMetrics;
      }
      for(Metric m: record.metrics()){
        setAttrCacheMetric(m, recNo);  
        ++numMetrics;
      }
      ++recNo;
    } 
    return numMetrics;
  }

  private void setAttrCacheTag(MetricsTag t, int recNo){
    String key = "tag."+ t.name() + ((0<recNo)?"."+recNo:"");
    attrCache.put(key, new Attribute(key, t.value()));   
  }

  private void setAttrCacheMetric(Metric m, int recNo){
    String key = m.name() + ((0<recNo)?"."+recNo:"");
    attrCache.put(key, new Attribute(key, m.value()));   
  }


  @Override
  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
             MBeanException, ReflectionException {
    throw new UnsupportedOperationException("Metrics are read-only.");
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    throw new UnsupportedOperationException("Metrics are read-only.");
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature)
      throws MBeanException, ReflectionException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  String name(){
    return this.name;
  }

  String description(){
    return this.description;
  }

  public MetricsSource source(){
    return this.source;
  }
  
}
