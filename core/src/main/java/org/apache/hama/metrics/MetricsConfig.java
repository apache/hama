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

import java.io.InputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.ReflectionUtils;

public final class MetricsConfig {

  public static final Log LOG = LogFactory.getLog(MetricsConfig.class); 

  public static final String DEFAULT_METRICS_CONFIG = "hama-metrics.properties";
  private final Properties properties;

  public static final class Entry<KEY, VALUE>{
    private final KEY key;
    private VALUE value;
    Entry(KEY key, VALUE value){
      this.key = key;
      this.value = value;
    } 
    public KEY key(){
      return this.key;
    }
    public VALUE value(){
      return this.value;
    }
  }

  /**
   * Internal use only. The purpose is to filter out unnecessary key value pair.
   */
  abstract class Filter {
    public abstract void doFilter(String key, String value);
  }

  MetricsConfig(Properties properties){
    this.properties = properties;
  }

  /**
   * Create MetricsConfig with HamaConfiguration file specified.
   * Within HamaConfiguration file, `bsp.metrics.conf_files' can specify
   * multiple config files with comma as dilemeter. It loads with whichever 
   * succeeded first.
   * @param conf file contains multiple metrics properties configuration files.
   * @return MetricsConfig file which contains all properties.
   */ 
  public static MetricsConfig create(HamaConfiguration conf){
     String configFiles = 
       conf.get("bsp.metrics.conf_files", DEFAULT_METRICS_CONFIG);
     return loadFirst(conf, configFiles.split(",")); 
  }  

  static MetricsConfig loadFirst(HamaConfiguration conf, String... fileNames) {
    StringBuilder names = new StringBuilder();
    for (String name : fileNames) {
      InputStream stream = null;
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if(null != loader) { 
        stream = loader.getSystemResourceAsStream(name.trim());
      }
      if(null != stream) {
        Properties prop = new Properties();
        try{
          prop.load(stream);
        }catch(IOException ioe){
          names.append(name+" "); 
          LOG.warn("Can not load properties from file "+name, ioe); 
        }
        return new MetricsConfig(prop);
      }else{
        names.append(name+" "); 
        LOG.warn("Can not load properties from file "+name);
      }
    }
    throw new RuntimeException("Can not load configuration files "+
          names.toString());
  }

  /**
   * Return all properties stored as MetricsConfig.Entry. It is accessible 
   * by calling key() and value() for obtaining key, value data respectively. 
   * @return Set that contains all properties.
   */
  public Set<Entry<String, String>> entrySet(){
    final Set<Entry<String, String>> set = new HashSet<Entry<String, String>>();
    list(new Filter(){
      public void doFilter(String key, String value){
        set.add(new Entry(key, value));
      }
    });  
    return set;
  }

  private void list(Filter filter){
    for(Enumeration e = properties.propertyNames(); e.hasMoreElements(); ){
      String key = (String)e.nextElement();
      String value = properties.getProperty(key); 
      filter.doFilter(key, value);
    }
  }

  private Set<Entry<String, String>> filter(String pattern){
    final Pattern p = Pattern.compile(pattern); 
    final Set<Entry<String, String>> set = 
      new HashSet<Entry<String, String>>();
    list(new Filter(){
      public void doFilter(String key, String value){
        Matcher m = p.matcher(key);    
        if(m.matches()){
           set.add(new Entry(key, value));
        }
      }
    }); 
    return set;
  }

  /**
   * Default delimeter set to `.'
   */
  public Set<Entry<String, String>> subset(String key) {
    return subset(key, '.');
  }
  
  public Set<Entry<String, String>> subset(String key, 
      char delimeter){
    String[] array = key.split("\\"+delimeter);
    if(LOG.isDebugEnabled()){
      LOG.debug("array length:"+array.length);
    }
    return subset(array);
  }

  public Set<Entry<String, String>> subset(String ... keys){
    int pos = 0;
    StringBuilder pattern = new StringBuilder();
    for(String key: keys){
      if(0==pos){ 
        pattern.append("^("+key+")");
      }else {
        pattern.append("\\.("+key+")");
      }
      pos++;
    }
    pattern.append("\\..+");
    if(LOG.isDebugEnabled()){
      LOG.debug("Pattern string: "+pattern.toString());
    }
    return filter(pattern.toString()); 
  }

  /**
   * List all keys without filter any key in properties file.
   * @return Set that contains all keys.
   */
  public Set<String> keys(){
    final Set<String> keys = new HashSet<String>();
    list(new Filter(){
      public void doFilter(String key, String value){
        keys.add(key);
      }
    });
    return keys;
  }
  
  /**
   * List all values without filter any value in properties file.
   * @return Set that contains all values.
   */
  public Set<String> values(){
    final Set<String> values = new HashSet<String>();
    list(new Filter(){
      public void doFilter(String key, String value){
        values.add(value);
      }
    });
    return values;
  } 

  /**
   * Retrieve metrics filter according to the name provided.
   * @param name name of the filter.  
   * @return MetricsFilter to be obtained.
   */
  public MetricsFilter getFilter(String name){ // default.source.filter
    if(null == name) return null;
    if(name.isEmpty()) return null;
    String[] array = name.split("\\.");
    if(3 != array.length) {
      LOG.warn("Name is not in a valid form of [prefix].[source|sink].[filter]"+
               name);
      return null;
    }
    Set<Entry<String, String>> sub = subset(array);
    if(null == sub) return null;
    for(Entry<String, String> e: sub){
      if((e.key()).equals(name+".class")){
         try {
           return (MetricsFilter)ReflectionUtils.newInstance(e.value());
         }catch(ClassNotFoundException cnfe){
           LOG.warn("Can not load filter from properties file.", cnfe);
         }
      }
    }
    return new GlobalFilter(subset(array[0], array[1]));
  }

}
