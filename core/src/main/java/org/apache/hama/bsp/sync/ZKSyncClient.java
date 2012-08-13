/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * A Zookeeper based BSP distributed synchronization client that provides
 * primitive synchronization API's
 * 
 */
public abstract class ZKSyncClient implements SyncClient, Watcher {

  Log LOG = LogFactory.getLog(ZKSyncClient.class);

  private ZooKeeper zk;
  private String bspRoot;

  protected Map<String, List<ZKSyncEventListener>> eventListenerMap;

  public ZKSyncClient() {
    eventListenerMap = new HashMap<String, List<ZKSyncEventListener>>(10);
  }

  /**
   * Initializes the zookeeper client-base.
   */
  protected void initialize(ZooKeeper zookeeper, String root) {
    LOG.info("Initializing ZK Sync Client");
    zk = zookeeper;
    bspRoot = root;
    eventListenerMap = new HashMap<String, List<ZKSyncEventListener>>(10);
  }

  /**
   * Returns the Node name using conventions to address a superstep progress for
   * task
   * 
   * @param taskId The Task ID
   * @param superstep The superstep number
   * @return String that represents the Zookeeper node path that could be
   *         created.
   */
  protected String getNodeName(TaskAttemptID taskId, long superstep) {
    return constructKey(taskId.getJobID(), "sync", "" + superstep,
        taskId.toString());
    //
    // bspRoot + "/" + taskId.getJobID().toString() + "/" + superstep + "/"
    // + taskId.toString();
  }

  private String correctKey(String key) {
    if (!key.startsWith("/")) {
      key = "/" + key;
    }
    return key;
  }

  /**
   * Check if the zookeeper node exists.
   * 
   * @param path The Zookeeper node path to check.
   * @param watcher A Watcher that would trigger interested events on the node.
   *          This value could be null if no watcher has to be left.
   * @return true if the node exists.
   * @throws KeeperException
   * @throws InterruptedException
   */
  protected boolean isExists(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    synchronized (zk) {
      return !(null == zk.exists(path, false));
    }
  }

  /**
   * Returns the zookeeper stat object.
   * 
   * @param path The path of the expected Zookeeper node.
   * @return Stat object for the Zookeeper path.
   * @throws KeeperException
   * @throws InterruptedException
   */
  protected Stat getStat(final String path) throws KeeperException,
      InterruptedException {
    synchronized (zk) {
      return zk.exists(path, false);
    }
  }

  private void createZnode(final String path, final CreateMode mode,
      byte[] data, Watcher watcher) throws KeeperException,
      InterruptedException {

    synchronized (zk) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking node " + path);
      }
      Stat s = zk.exists(path, false);
      if (null == s) {
        try {
          zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Created node " + path);
          }

        } catch (KeeperException.NodeExistsException nee) {
          LOG.debug("Ignore because znode may be already created at " + path,
              nee);
        }
      }
    }
  }

  /**
   * Utility function to get byte array out of Writable
   * 
   * @param value The Writable object to be converted to byte array.
   * @return byte array from the Writable object. Returns null on given null
   *         value or on error.
   */
  protected byte[] getBytesForData(Writable value) {
    byte[] data = null;

    if (value != null) {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream outputStream = new DataOutputStream(byteStream);
      try {
        value.write(outputStream);
        outputStream.flush();
        data = byteStream.toByteArray();
      } catch (IOException e) {
        LOG.error("Error writing data to write buffer.", e);
      } finally {
        try {
          byteStream.close();
          outputStream.close();
        } catch (IOException e) {
          LOG.error("Error closing byte stream.", e);
        }
      }
    }
    return data;
  }

  /**
   * Utility function to read Writable object value from byte array.
   * 
   * @param data The byte array
   * @param classType The Class object of expected Writable object.
   * @return The instance of Writable object.
   * @throws IOException
   */
  protected boolean getValueFromBytes(byte[] data,
      Writable valueHolder) throws IOException {
    if (data != null) {
      ByteArrayInputStream istream = new ByteArrayInputStream(data);
      DataInputStream diStream = new DataInputStream(istream);
      try {
        valueHolder.readFields(diStream);
      } finally {
        diStream.close();
      }
      return true;
    }
    return false;
  }

  /**
   * Read value stored in the Zookeeper node.
   * 
   * @param path The path of the Zookeeper node.
   * @param classType The expected class type of the Writable object.
   * @return The Writable object constructed from the value read from the
   *         Zookeeper node.
   */
  protected boolean extractData(String path,
      Writable valueHolder) {
    try {
      Stat stat = getStat(path);
      if (stat != null) {
        byte[] data = this.zk.getData(path, false, stat);
        try {
          getValueFromBytes(data, valueHolder);
        } catch (IOException e) {
          LOG.error(
              new StringBuffer(200).append("Error getting data from path ")
                  .append(path).toString(), e);
          return false;
        }
        return true;
      }

    } catch (KeeperException e) {
      LOG.error(new StringBuilder(200).append("Error checking zk path ")
          .append(path).toString(), e);

    } catch (InterruptedException e) {
      LOG.error(new StringBuilder(200).append("Error checking zk path ")
          .append(path).toString(), e);

    }
    return false;

  }

  /**
   * Writes data into the Zookeeper node. If the path does not exist the
   * zookeeper node is created recursively and the value is stored in the node.
   * 
   * @param path The path of the Zookeeper node.
   * @param value The value to be stored in the Zookeeper node.
   * @param persistent true if the node to be created is ephemeral of permanent
   * @param watcher If any Watcher object should listen to event on the
   *          Zookeeper node.
   * @return true if operation is successful.
   */
  protected boolean writeNode(String path, Writable value, boolean persistent,
      Watcher watcher) {
    if (path == null || "".equals(path.trim())) {
      return false;
    }
    path = correctKey(path);
    boolean pathExists = false;
    try {
      pathExists = isExists(path, watcher);
    } catch (KeeperException e) {
      LOG.error(new StringBuilder(200).append("Error checking zk path ")
          .append(path).toString(), e);
    } catch (InterruptedException e) {
      LOG.error(new StringBuilder(200).append("Error checking zk path ")
          .append(path).toString(), e);
    }

    byte[] data = getBytesForData(value);

    if (!pathExists) {
      try {
        String[] pathComponents = path.split("/");
        StringBuffer pathBuffer = new StringBuffer(path.length()
            + pathComponents.length);
        for (int i = 0; i < pathComponents.length - 1; ++i) {
          if (pathComponents[i].equals(""))
            continue;
          pathBuffer.append("/").append(pathComponents[i]);
          createZnode(pathBuffer.toString(), CreateMode.PERSISTENT, null,
              watcher);
        }
        pathBuffer.append("/")
            .append(pathComponents[pathComponents.length - 1]);
        CreateMode mode = CreateMode.EPHEMERAL;
        if (persistent) {
          mode = CreateMode.PERSISTENT;
        }
        createZnode(pathBuffer.toString(), mode, data, watcher);

        return true;
      } catch (InterruptedException e) {
        LOG.error(new StringBuilder(200).append("Error creating zk path ")
            .append(path).toString(), e);
      } catch (KeeperException e) {
        LOG.error(new StringBuilder(200).append("Error creating zk path ")
            .append(path).toString(), e);
      }
    } else if (value != null) {
      try {
        this.zk.setData(path, data, -1);
        return true;
      } catch (InterruptedException e) {
        LOG.error(new StringBuilder(200).append("Error modifying zk path ")
            .append(path).toString(), e);
        return false;
      } catch (KeeperException e) {
        LOG.error(new StringBuilder(200).append("Error modifying zk path ")
            .append(path).toString(), e);
      }
    }
    return false;
  }

  @Override
  public String constructKey(BSPJobID jobId, String... args) {
    StringBuffer keyBuffer = new StringBuffer(100);
    keyBuffer.append(bspRoot);
    if (jobId != null)
      keyBuffer.append("/").append(jobId.toString());
    for (String arg : args) {
      keyBuffer.append("/").append(arg);
    }
    return keyBuffer.toString();
  }

  @Override
  public boolean storeInformation(String key, Writable value,
      boolean permanent, SyncEventListener listener) {
    ZKSyncEventListener zkListener = (ZKSyncEventListener) listener;
    key = correctKey(key);
    final String path = key;
    LOG.info("Writing data " + path);
    return writeNode(path, value, permanent, zkListener);
  }

  @Override
  public boolean getInformation(String key, Writable valueHolder) {
    key = correctKey(key);
    final String path = key;
    return extractData(path, valueHolder);
  }

  @Override
  public boolean addKey(String key, boolean permanent,
      SyncEventListener listener) {
    ZKSyncEventListener zkListener = (ZKSyncEventListener) listener;
    return writeNode(key, null, permanent, zkListener);
  }

  @Override
  public boolean hasKey(String key) {
    try {
      return isExists(key, null);
    } catch (KeeperException e) {
      LOG.error(new StringBuilder(200).append("Error checking zk path ")
          .append(key).toString(), e);
    } catch (InterruptedException e) {
      LOG.error(new StringBuilder(200).append("Error checking zk path ")
          .append(key).toString(), e);
    }
    return false;
  }

  @Override
  public boolean registerListener(String key, SyncEvent event,
      SyncEventListener listener) {
    key = correctKey(key);

    LOG.debug("Registering listener for " + key);
    ZKSyncEventListener zkListener = (ZKSyncEventListener) listener;
    zkListener.setSyncEvent(event);
    zkListener.setZKSyncClient(this);
    synchronized (this.zk) {

      try {
        Stat stat = this.zk.exists(key, zkListener);
        if (stat == null) {
          writeNode(key, null, true, zkListener);
        }
        this.zk.getData(key, zkListener, stat);
        this.zk.getChildren(key, zkListener);
        // List<ZKSyncEventListener> list = this.eventListenerMap.get(key);
        // if(!eventListenerMap.containsKey(key)){
        // list = new ArrayList<ZKSyncEventListener>(5);
        // }
        // list.add(zkListener);
        // this.eventListenerMap.put(key, list);
        return true;
      } catch (KeeperException e) {
        LOG.error("Error getting stat and data.", e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted getting stat and data.", e);
      }

    }
    return false;
  }

  @Override
  public String[] getChildKeySet(String key, SyncEventListener listener) {
    key = correctKey(key);
    ZKSyncEventListener zkListener = null;
    if (listener != null) {
      zkListener = (ZKSyncEventListener) listener;
    }
    Stat stat = null;
    String[] children = new String[0];
    try {
      stat = this.zk.exists(key, null);

    } catch (KeeperException e) {
      LOG.error("Error getting stat and data.", e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted getting stat and data.", e);
    }
    if (stat == null)
      return children;

    try {
      List<String> childList = this.zk.getChildren(key, zkListener);
      children = new String[childList.size()];
      childList.toArray(children);
    } catch (KeeperException e) {
      LOG.error("Error getting stat and data.", e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted getting stat and data.", e);
    }

    return children;
  }

  /**
   * Clears all sub-children of node bspRoot
   */
  protected void clearZKNodes() {
    try {
      Stat s = zk.exists(bspRoot, false);
      if (s != null) {
        clearZKNodes(bspRoot);
      }

    } catch (Exception e) {
      LOG.warn("Could not clear zookeeper nodes.", e);
    }
  }

  /**
   * Clears all sub-children of node rooted at path.
   * 
   * @param path
   * @throws InterruptedException
   * @throws KeeperException
   */
  protected void clearZKNodes(String path) throws KeeperException,
      InterruptedException {
    ArrayList<String> list = (ArrayList<String>) zk.getChildren(path, false);

    if (list.size() == 0) {
      return;

    } else {
      for (String node : list) {
        clearZKNodes(path + "/" + node);
        LOG.debug("Deleting " + path + "/" + node);
        zk.delete(path + "/" + node, -1); // delete any version of this
        // node.
      }
    }
  }

  @Override
  public void process(WatchedEvent arg0) {
  }

  @Override
  public boolean remove(String key, SyncEventListener listener) {
    key = correctKey(key);
    try {
      clearZKNodes(key);
      this.zk.delete(key, -1);
      return true;
    } catch (KeeperException e) {
      LOG.error("Error deleting key " + key);
    } catch (InterruptedException e) {
      LOG.error("Error deleting key " + key);
    }
    return false;

  }

  @Override
  public void close() throws IOException {

    try {
      this.zk.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

  }

}
