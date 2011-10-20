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
package org.apache.hama.checkpoint;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.GroomServer.CheckpointerChild;


public final class CheckpointRunner implements Callable<Object> {

  public static final Log LOG = LogFactory.getLog(CheckpointRunner.class);
  public static final String DEFAULT_PORT = "1590";
  
  private final List<String> commands;
  private final ScheduledExecutorService sched;
  private final AtomicReference<Process> process;
  private final AtomicBoolean isAlive = new AtomicBoolean(false);

  public CheckpointRunner(List<String> commands) {
    this.commands = commands;
    if(LOG.isDebugEnabled()) {
      LOG.debug("Command for executing Checkpoint runner:"+
      Arrays.toString(this.commands.toArray()));
    }
    this.sched = Executors.newScheduledThreadPool(10);
    this.process = new AtomicReference<Process>();
  }

  public static final List<String> buildCommands(final Configuration config) {
    List<String> vargs = new ArrayList<String>();
    File jvm =
      new File(new File(System.getProperty("java.home"), "bin"), "java");
    vargs.add(jvm.toString());

    String javaOpts = config.get("bsp.checkpoint.child.java.opts", "-Xmx50m");
    String[] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }
    vargs.add("-classpath");
    vargs.add(System.getProperty("java.class.path"));
    vargs.add(CheckpointerChild.class.getName());
    String port = config.get("bsp.checkpoint.port", DEFAULT_PORT);
    if(LOG.isDebugEnabled())
      LOG.debug("Checkpointer's port:"+port);
    vargs.add(port);

    return vargs;
  }

  public void start() {
    if(!isAlive.compareAndSet(false, true)) {
      throw new IllegalStateException(this.getClass().getName()+
      " is already running.");
    }
    this.sched.schedule(this, 0, SECONDS);
    LOG.info("Start building checkpointer process.");
  }

  public void stop() {
    kill();
    this.sched.shutdown();
    LOG.info("Stop checkpointer process.");
  }

  public Process getProcess() {
    return this.process.get();
  }

  public void kill() {
    if (this.process.get() != null) {
      this.process.get().destroy();
    }
    isAlive.set(false);
  }

  public boolean isAlive() {
    return isAlive.get();
  }

  public Object call() throws Exception {
    ProcessBuilder builder = new ProcessBuilder(commands);
    try{
      this.process.set(builder.start());
      new Thread() {
        @Override
        public void run() {
          logStream(process.get().getErrorStream());
        }
      }.start();

      new Thread() {
        @Override
        public void run() {
          logStream(process.get().getInputStream());
        }
      }.start();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOG.info("Destroying checkpointer process.");
          getProcess().destroy();
        }
      });

      int exit_code = this.process.get().waitFor();
      if (!isAlive() && exit_code != 0) {
        throw new IOException("Checkpointer process exit with nonzero status of "
            + exit_code + ".");
      }
    } catch(InterruptedException e){
      LOG.warn("Thread is interrupted when execeuting Checkpointer process.", e);
    } catch(IOException ioe) {
      LOG.error("Error when executing Checkpointer process.", ioe);
    } finally {
      kill();
    }
    return null;
  }

  private void logStream(InputStream output) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(output));
      String line;
      while ((line = in.readLine()) != null) {
        LOG.info(line);
      }
    } catch (IOException e) {
      LOG.warn("Error reading checkpoint process's inputstream.", e);
    } finally {
      try {
        output.close();
      } catch (IOException e) {
        LOG.warn("Error closing checkpoint's inputstream.", e);
      }
    }
  }
}
