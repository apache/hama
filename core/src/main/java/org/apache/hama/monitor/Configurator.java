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
package org.apache.hama.monitor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.monitor.Monitor.Task;
import org.apache.hama.monitor.Monitor.Result;
import org.apache.hama.HamaConfiguration;

/**
 * Configurator loads and configure jar files.  
 */
public final class Configurator {

  public static final Log LOG = LogFactory.getLog(Configurator.class); 
  public static final String DEFAULT_PLUGINS_DIR = "plugins";
  private static final ConcurrentMap<String, Long> repos = 
    new ConcurrentHashMap<String, Long>();

  /**
   * @param conf file points out the plugin dir location.
   * @return Map contains jar path and task to be executed.
   */ 
  public static Map<String, Task> configure(HamaConfiguration conf, 
      MonitorListener listener) throws IOException {
    String hamaHome = System.getProperty("hama.home.dir");
    String pluginPath = conf.get("bsp.monitor.plugins.dir", 
      hamaHome+File.separator+DEFAULT_PLUGINS_DIR);
    File pluginDir = new File(pluginPath);
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Map<String, Task> taskList = new HashMap<String, Task>();
    LOG.info("Scanning jar files within "+pluginDir+".");
    for(File jar: pluginDir.listFiles()) {
      String jarPath = jar.getPath();
      Long timestamp = repos.get(jarPath);
      if(null == timestamp || jar.lastModified() > timestamp) {
        Task t = load(jar, loader);
        if(null != t) {
          t.setListener(listener);
          taskList.put(jarPath, t);
          repos.put(jarPath, new Long(jar.lastModified()));
          LOG.info(jar.getName()+" is loaded.");
        }
      }
    }
    return taskList;
  }

  /**
   * Load jar from specified path.
   * @param path to the jar file.
   * @param loader of the current thread.  
   * @return task to be run.
   */
  private static Task load(File path, ClassLoader loader) throws IOException {
    JarFile jar = new JarFile(path);
    Manifest manifest = jar.getManifest();
    String pkg = manifest.getMainAttributes().getValue("Package");
    String main = manifest.getMainAttributes().getValue("Main-Class");
    if(null == pkg || null == main ) 
      throw new NullPointerException("Package or main class not found "+
      "in menifest file.");
    String namespace = pkg + File.separator + main;
    namespace = namespace.replaceAll(File.separator, ".");
    LOG.info("Task class to be loaded: "+namespace);
    URLClassLoader child = 
      new URLClassLoader(new URL[]{path.toURI().toURL()}, loader); 
    Thread.currentThread().setContextClassLoader(child);
    Class<?> taskClass = null;
    try {
      taskClass = Class.forName(namespace, true, child); // task class
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Task class is not found.", cnfe);
    }
    if(null == taskClass) return null;

    try {
      return (Task)taskClass.newInstance();
    } catch(InstantiationException ie) {
      LOG.warn("Unable to instantiate task class."+namespace, ie);
    } catch(IllegalAccessException iae) {
      LOG.warn(iae);
    }
    return null;
  }
  
}
