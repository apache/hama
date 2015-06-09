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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

/**
 * Refelction utility for BSP programmes.
 */
public class ReflectionUtils {

  public static final Log LOG = LogFactory.getLog(ReflectionUtils.class);

  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<Class<?>, Constructor<?>>();

  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className)
      throws ClassNotFoundException {
    return newInstance((Class<T>) Class.forName(className));
  }

  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass) {
    Preconditions.checkNotNull(theClass);
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (null == meth) {
        meth = theClass.getDeclaredConstructor(new Class[0]);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Create an instance using class literal name and object values supplied.
   * 
   * @param className is the string name of the class to be created.
   * @param values supplied in object array.
   * @exception ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className, Object[] values)
      throws ClassNotFoundException {
    return newInstance((Class<T>) Class.forName(className), values);
  }

  /**
   * Create an instance with corresponded class and object values supplied.
   * Constructor
   * 
   * @param theClass supplies instance to be created.
   * @param values are parameters applied when instance is created.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> T newInstance(Class<T> theClass, Object[] values) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (null == meth) {
        Class[] parameters = new Class[values.length];
        int idx = 0;
        for (Object value : values) {
          parameters[idx++] = value.getClass();
        }
        meth = theClass.getDeclaredConstructor(parameters);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(values);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;

  }

  /**
   * Create an instance with corresponded class and object values supplied.
   * Constructor
   * 
   * @param theClass supplies instance to be created.
   * @param parameters are class type of object values supplied.
   * @param values are parameters applied when instance is created.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> T newInstance(Class<T> theClass, Class[] parameters,
      Object[] values) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (null == meth) {
        meth = theClass.getDeclaredConstructor(parameters);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(values);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  static private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  /**
   * Print all of the thread's information and stack traces.
   * 
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  public synchronized static void printThreadInfo(PrintWriter stream,
      String title) {
    final int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread "
          + getTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state);
      stream.println("  Blocked count: " + info.getBlockedCount());
      stream.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime());
        stream.println("  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName());
        stream.println("  Blocked by "
            + getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }

  private static long previousLogTime = 0;

  /**
   * Log the current thread stacks at INFO level.
   * 
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last
   */
  public static void logThreadInfo(Log log, String title, long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        long now = System.currentTimeMillis();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        printThreadInfo(new PrintWriter(buffer), title);
        log.info(buffer.toString());
      }
    }
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }
}
