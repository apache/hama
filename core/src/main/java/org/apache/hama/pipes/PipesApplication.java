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
package org.apache.hama.pipes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskLog;
import org.apache.hama.pipes.protocol.BinaryProtocol;
import org.apache.hama.pipes.protocol.DownwardProtocol;
import org.apache.hama.pipes.protocol.StreamingProtocol;

/**
 * This class is responsible for launching and communicating with the child
 * process.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class PipesApplication<K1, V1, K2, V2, M extends Writable> {
  private static final Log LOG = LogFactory.getLog(PipesApplication.class);
  private static final int SERVER_SOCKET_TIMEOUT = 2000;
  private ServerSocket serverSocket;
  private Process process;
  private Socket clientSocket;

  private DownwardProtocol<K1, V1, K2, V2> downlink;
  private boolean streamingEnabled = false;

  static final boolean WINDOWS = System.getProperty("os.name").startsWith(
      "Windows");

  public PipesApplication() {
  }

  /* Build Environment based on the Configuration */
  public Map<String, String> setupEnvironment(Configuration conf)
      throws IOException {

    Map<String, String> env = new HashMap<String, String>();

    this.streamingEnabled = conf.getBoolean("hama.streaming.enabled", false);

    if (!this.streamingEnabled) {
      serverSocket = new ServerSocket(0);
      env.put("hama.pipes.command.port",
          Integer.toString(serverSocket.getLocalPort()));
    }

    // add TMPDIR environment variable with the value of java.io.tmpdir
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));

    // Set Logging Environment from Configuration
    env.put("hama.pipes.logging",
        conf.getBoolean("hama.pipes.logging", false) ? "1" : "0");

    return env;
  }

  /* Build a Command String based on the Configuration */
  private List<String> setupCommand(Configuration conf) throws IOException,
      InterruptedException {

    List<String> cmd = new ArrayList<String>();
    String interpretor = conf.get("hama.pipes.executable.interpretor");
    if (interpretor != null) {
      cmd.add(interpretor);
    }

    String executable = null;
    try {
      if (DistributedCache.getLocalCacheFiles(conf) != null) {
        LOG.debug("DEBUG LocalCacheFilesCount: "
            + DistributedCache.getLocalCacheFiles(conf).length);
        for (Path u : DistributedCache.getLocalCacheFiles(conf))
          LOG.debug("DEBUG LocalCacheFiles: " + u);

        executable = DistributedCache.getLocalCacheFiles(conf)[0].toString();

        LOG.debug("DEBUG: executable: " + executable);
      } else {
        LOG.debug("DEBUG: DistributedCache.getLocalCacheFiles(conf) returns null.");
        throw new IOException("Executable is missing!");
      }
    } catch (Exception e) {
      LOG.error("Executable: " + executable + " fs.default.name: "
          + conf.get("fs.default.name"));

      throw new IOException("Executable is missing!");
    }

    if (!new File(executable).canExecute()) {
      // LinuxTaskController sets +x permissions on all distcache files already.
      // In case of DefaultTaskController, set permissions here.
      FileUtil.chmod(executable, "u+x");
    }

    cmd.add(executable);

    String additionalArgs = conf.get("hama.pipes.executable.args");
    // if true, we are resolving filenames with the linked paths in
    // DistributedCache
    boolean resolveArguments = conf.getBoolean(
        "hama.pipes.resolve.executable.args", false);
    if (additionalArgs != null && !additionalArgs.isEmpty()) {
      String[] split = additionalArgs.split(" ");
      for (String s : split) {
        if (resolveArguments) {
          for (Path u : DistributedCache.getLocalCacheFiles(conf)) {
            if (u.getName().equals(s)) {
              LOG.info("Resolved argument \"" + s
                  + "\" with fully qualified path \"" + u.toString() + "\"!");
              cmd.add(u.toString());
              break;
            }
          }
        } else {
          cmd.add(s);
        }
      }
    }

    return cmd;
  }

  /* If Parent File/Folder doesn't exist, build one. */
  private void checkParentFile(File file) {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
      LOG.debug("File: " + file.getParentFile().getAbsolutePath() + " created!");
    }
  }

  /**
   * Start the child process to handle the task for us. Peer is not available
   * now, start child by using only the configuration! e.g., PipesPartitioner,
   * no peer is available at this time!
   * 
   * @param conf task's configuration
   * @throws InterruptedException
   * @throws IOException
   */
  public void start(Configuration conf) throws IOException,
      InterruptedException {

    Map<String, String> env = setupEnvironment(conf);
    List<String> cmd = setupCommand(conf);

    // wrap the command in a stdout/stderr capture
    File stdout = TaskLog.getLocalTaskLogFile(TaskLog.LogName.STDOUT,
        "yyyyMMdd'_partitioner_'HHmmss");
    File stderr = TaskLog.getLocalTaskLogFile(TaskLog.LogName.STDERR,
        "yyyyMMdd'_partitioner_'HHmmss");

    // Get the desired maximum length of task's logs.
    long logLength = TaskLog.getTaskLogLength(conf);

    if (!streamingEnabled) {
      cmd = TaskLog.captureOutAndError(null, cmd, stdout, stderr, logLength);
    } else {
      // use tee in streaming to get the output to file
      cmd = TaskLog.captureOutAndErrorTee(null, cmd, stdout, stderr, logLength);
    }

    /* Check if Parent folders for STDOUT exist */
    checkParentFile(stdout);
    LOG.debug("STDOUT: " + stdout.getAbsolutePath());
    /* Check if Parent folders for STDERR exist */
    checkParentFile(stderr);
    LOG.debug("STDERR: " + stderr.getAbsolutePath());

    LOG.debug("DEBUG: cmd: " + cmd);
    process = runClient(cmd, env); // fork c++ binary

    try {
      if (!streamingEnabled) {
        LOG.debug("DEBUG: waiting for Client at "
            + serverSocket.getLocalSocketAddress());
        serverSocket.setSoTimeout(SERVER_SOCKET_TIMEOUT);
        clientSocket = serverSocket.accept();
        LOG.debug("DEBUG: Client connected! - start BinaryProtocol!");

        downlink = new BinaryProtocol<K1, V1, K2, V2, M>(conf,
            clientSocket.getOutputStream(), clientSocket.getInputStream());

        downlink.start();
      }

    } catch (SocketException e) {
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(stderr)));

      String inputLine;
      while ((inputLine = br.readLine()) != null) {
        LOG.error("PipesApp Error: " + inputLine);
      }
      br.close();

      throw new SocketException(
          "Timout: Client pipes application did not connect!");
    }
  }

  /**
   * Start the child process to handle the task for us.
   * 
   * @param peer the current peer including the task's configuration
   * @throws InterruptedException
   * @throws IOException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void start(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      InterruptedException {

    Map<String, String> env = setupEnvironment(peer.getConfiguration());
    List<String> cmd = setupCommand(peer.getConfiguration());

    // wrap the command in a stdout/stderr capture
    TaskAttemptID taskid = peer.getTaskId();
    File stdout = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR);

    // Get the desired maximum length of task's logs.
    long logLength = TaskLog.getTaskLogLength(peer.getConfiguration());

    if (!streamingEnabled) {
      cmd = TaskLog.captureOutAndError(null, cmd, stdout, stderr, logLength);
    } else {
      // use tee in streaming to get the output to file
      cmd = TaskLog.captureOutAndErrorTee(null, cmd, stdout, stderr, logLength);
    }

    /* Check if Parent folders for STDOUT exist */
    checkParentFile(stdout);
    LOG.debug("STDOUT: " + stdout.getAbsolutePath());
    /* Check if Parent folders for STDERR exist */
    checkParentFile(stderr);
    LOG.debug("STDERR: " + stderr.getAbsolutePath());

    LOG.debug("DEBUG: cmd: " + cmd);
    process = runClient(cmd, env); // fork c++ binary

    try {
      if (streamingEnabled) {
        downlink = new StreamingProtocol(peer, process.getOutputStream(),
            process.getInputStream());
      } else {
        LOG.debug("DEBUG: waiting for Client at "
            + serverSocket.getLocalSocketAddress());
        serverSocket.setSoTimeout(SERVER_SOCKET_TIMEOUT);
        clientSocket = serverSocket.accept();
        LOG.debug("DEBUG: Client connected! - start BinaryProtocol!");

        downlink = new BinaryProtocol<K1, V1, K2, V2, M>(peer,
            clientSocket.getOutputStream(), clientSocket.getInputStream());
      }

      downlink.start();

    } catch (SocketException e) {
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(stderr)));

      String inputLine;
      while ((inputLine = br.readLine()) != null) {
        LOG.error("PipesApp Error: " + inputLine);
      }
      br.close();

      throw new SocketException(
          "Timout: Client pipes application did not connect!");
    }
  }

  /**
   * Get the downward protocol object that can send commands down to the
   * application.
   * 
   * @return the downlink proxy
   */
  DownwardProtocol<K1, V1, K2, V2> getDownlink() {
    return downlink;
  }

  /**
   * Wait for the application to finish
   * 
   * @return did the application finish correctly?
   * @throws InterruptedException
   * @throws IOException
   */
  boolean waitForFinish() throws InterruptedException, IOException {
    downlink.flush();
    return downlink.waitForFinish();
  }

  /**
   * Abort the application and wait for it to finish.
   * 
   * @param t the exception that signalled the problem
   * @throws IOException A wrapper around the exception that was passed in
   */
  void abort(Throwable t) throws IOException {
    LOG.info("Aborting because of " + StringUtils.stringifyException(t));
    try {
      downlink.abort();
      downlink.flush();
    } catch (IOException e) {
      // IGNORE cleanup problems
    }
    try {
      downlink.waitForFinish();
    } catch (Throwable ignored) {
      process.destroy();
    }
    IOException wrapper = new IOException("pipe child exception");
    wrapper.initCause(t);
    throw wrapper;
  }

  /**
   * Clean up the child process and socket if exist.
   * 
   * @throws IOException
   */
  public void cleanup(boolean sendClose) throws IOException {
    if (serverSocket != null) {
      serverSocket.close();
    }
    try {
      if ((downlink != null) && (sendClose)) {
        downlink.close();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Run a given command in a subprocess, including threads to copy its stdout
   * and stderr to our stdout and stderr.
   * 
   * @param command the command and its arguments
   * @param env the environment to run the process in
   * @return a handle on the process
   * @throws IOException
   */
  static Process runClient(List<String> command, Map<String, String> env)
      throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    if (env != null) {
      builder.environment().putAll(env);
    }
    Process result = builder.start();
    return result;
  }

}
