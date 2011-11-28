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
package org.apache.hama.util;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Refelction utility for BSP programmes.
 */
public class ReflectionUtils {

  public static final Log LOG = LogFactory.getLog(ReflectionUtils.class);

  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = 
    new ConcurrentHashMap<Class<?>, Constructor<?>>();

  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className) 
      throws ClassNotFoundException {
    T result;
    try{
      Class<T> theClass = (Class<T>)Class.forName(className);
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if(null == meth) {
        meth = theClass.getDeclaredConstructor(new Class[0]);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    }catch(Exception e){
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Create an instance using class literal name and object values supplied.
   * @param className is the string name of the class to be created.
   * @param values supplied in object array.
   * @exception ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className, Object[] values) 
      throws ClassNotFoundException{
    return newInstance((Class<T>)Class.forName(className), values);
  }

  /**
   * Create an instance with corresponded class and object values supplied. Constructor
   * @param theClass supplies instance to be created.
   * @param values are parameters applied when instance is created.
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Object[] values) {
    T result;
    try{
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (null == meth) {
        Class[] parameters = new Class[values.length];
        int idx = 0;
        for(Object value: values){
          parameters[idx++] = value.getClass();
        }     
        meth = theClass.getDeclaredConstructor(parameters);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(values);
    }catch(Exception e){
      throw new RuntimeException(e);
    }
    return result;

  }

  /**
   * Create an instance with corresponded class and object values supplied. Constructor
   * @param theClass supplies instance to be created.
   * @param parameters are class type of object values supplied.
   * @param values are parameters applied when instance is created.
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Class[] parameters, 
      Object[] values) {
    T result;
    try{
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (null == meth) {
        meth = theClass.getDeclaredConstructor(parameters);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(values);
    }catch(Exception e){
      throw new RuntimeException(e);
    }
    return result;
  }

}
