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
package org.apache.hama.bsp;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.RunJar;

/** 
 * Base class that runs a task in a separate process. 
 */
public class TaskRunner extends Thread {

  public static final Log LOG = LogFactory.getLog(TaskRunner.class);

  boolean killed = false;
  private Process process;
  private Task task;
  private BSPJob conf;
  private GroomServer groomServer;

  public TaskRunner(BSPTask bspTask, GroomServer groom, BSPJob conf) {
    this.task = bspTask;
    this.conf = conf;
    this.groomServer = groom;
  }

  public Task getTask() {
    return task;
  }

  /**
   * Called to assemble this task's input. This method is run in the parent
   * process before the child is spawned. It should not execute user code, only
   * system code.
   */
  public boolean prepare() throws IOException {
    return true;
  }

  public void run() {
    try {
      String sep = System.getProperty("path.separator");
      File workDir = new File(new File(task.getJobFile()).getParent(), "work");
      boolean isCreated = workDir.mkdirs();
      if(!isCreated) {
        LOG.debug("TaskRunner.workDir : " + workDir);
      }

      StringBuffer classPath = new StringBuffer();
      // start with same classpath as parent process
      classPath.append(System.getProperty("java.class.path"));
      classPath.append(sep);

      String jar = conf.getJar();
      if (jar != null) { // if jar exists, it into workDir
        RunJar.unJar(new File(jar), workDir);
        File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
          for (int i = 0; i < libs.length; i++) {
            classPath.append(sep); // add libs from jar to classpath
            classPath.append(libs[i]);
          }
        }
        classPath.append(sep);
        classPath.append(new File(workDir, "classes"));
        classPath.append(sep);
        classPath.append(workDir);
      }

      // Build exec child jmv args.
      Vector<String> vargs = new Vector<String>();
      File jvm = // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
      vargs.add(jvm.toString());

      // bsp.child.java.opts
      String javaOpts = conf.getConf().get("bsp.child.java.opts", "-Xmx200m");
      javaOpts = javaOpts.replace("@taskid@", task.getTaskID().toString());
      
      String[] javaOptsSplit = javaOpts.split(" ");
      for (int i = 0; i < javaOptsSplit.length; i++) {
        vargs.add(javaOptsSplit[i]);
      }

      // Add classpath.
      vargs.add("-classpath");
      vargs.add(classPath.toString());
      // Add main class and its arguments
      vargs.add(GroomServer.Child.class.getName()); // main of Child

      InetSocketAddress addr = groomServer.getTaskTrackerReportAddress();
      vargs.add(addr.getHostName());
      vargs.add(Integer.toString(addr.getPort()));
      vargs.add(task.getTaskID().toString());

      // Run java
      runChild((String[]) vargs.toArray(new String[0]), workDir);
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  /**
   * Run the child process
   */
  private void runChild(String[] args, File dir) throws IOException {
    this.process = Runtime.getRuntime().exec(args, null, dir);
    try {
      new Thread() {
        public void run() {
          logStream(process.getErrorStream()); // copy log output
        }
      }.start();

      logStream(process.getInputStream()); // normally empty

      int exit_code = process.waitFor();
      if (!killed && exit_code != 0) {
        throw new IOException("Task process exit with nonzero status of "
            + exit_code + ".");
      }

    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      kill();
    }
  }

  /**
   * Kill the child process
   */
  public void kill() {
    if (process != null) {
      process.destroy();
    }
    killed = true;
  }

  /**
   */
  private void logStream(InputStream output) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(output));
      String line;
      while ((line = in.readLine()) != null) {
        LOG.info(task.getTaskID() + " " + line);
      }
    } catch (IOException e) {
      LOG.warn(task.getTaskID() + " Error reading child output", e);
    } finally {
      try {
        output.close();
      } catch (IOException e) {
        LOG.warn(task.getTaskID() + " Error closing child output", e);
      }
    }
  }

}
