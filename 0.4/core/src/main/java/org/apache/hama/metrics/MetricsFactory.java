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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.ReflectionUtils;


/**
 * Entry point to create MetricsSystem, MetricsSource, and MetricsSink.
 */
public final class MetricsFactory {
 
  public static final Log LOG = LogFactory.getLog(MetricsFactory.class);

  private static final ConcurrentMap<Class<?>, Object> cache = 
    new ConcurrentHashMap<Class<?>, Object>();

  /**
   * Create MetricsSystem.
   * @param conf supplies configuration parameters to MetricsSystem. 
   */
  public static final MetricsSystem createMetricsSystem(String prefix,
      final HamaConfiguration conf){
     MetricsSystem mc = new DefaultMetricsSystem(prefix, conf);
     cache.putIfAbsent(MetricsSystem.class, mc);
     return mc;
  }

  /**
   * Create MetricsSource.
   * @param clazz is the MetricsSource to be created.
   * @param values are an array as runtime parameters supplied to clazz.
   */
  public static final MetricsSource createSource(Class<?> clazz, 
      Object[] values){
    return createSource(clazz, values, false);
  }

  /**
   * Create MetricsSource.
   * @param clazz is the MetricsSource to be created.
   * @param params are class type of object values. 
   * @param values are an array as runtime parameters supplied to clazz.
   */
  public static final MetricsSource createSource(Class<?> clazz, Class[] params,
      Object[] values){
    return createSource(clazz, params, values, false);
  }

  /**
   * Create MetricsSource.
   * @param clazz is the MetricsSource to be created.
   * @param values are an array as runtime parameters supplied to clazz.
   * @param toCache indicates if the instance is to be cached. 
   */
  public static final MetricsSource createSource(Class<?> clazz, 
      Object[] values, boolean toCache){
    if(!MetricsSource.class.isAssignableFrom(clazz)){
      throw new IllegalArgumentException("Unknown source "+clazz.getName());
    }
    if(toCache){
      return (MetricsSource) cache.putIfAbsent(clazz, 
             ReflectionUtils.newInstance(clazz, values));
    }else{
      return (MetricsSource)ReflectionUtils.newInstance(clazz, values);
    }
  }

  /**
   * Create MetricsSource.
   * @param clazz is the MetricsSource to be created.
   * @param params are class type of object values. 
   * @param values are an array as runtime parameters supplied to clazz.
   * @param toCache indicates if the instance is to be cached. 
   */
  public static final MetricsSource createSource(Class<?> clazz, Class[] params, 
      Object[] values, boolean toCache){
    if(!MetricsSource.class.isAssignableFrom(clazz)){
      throw new IllegalArgumentException("Unknown source "+clazz.getName());
    }
    if(toCache){
      return (MetricsSource) cache.putIfAbsent(clazz, 
             ReflectionUtils.newInstance(clazz, params, values));
    }else{
      return (MetricsSource)ReflectionUtils.newInstance(clazz, params, values);
    }
  }

}
