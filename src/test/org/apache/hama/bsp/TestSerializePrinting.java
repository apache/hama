package org.apache.hama.bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Serialize Printing of Hello World
 */
public class TestSerializePrinting extends HamaCluster implements Watcher {
  private Log LOG = LogFactory.getLog(TestSerializePrinting.class);
  private int NUM_PEER = 10;
  List<BSPPeerThread> list = new ArrayList<BSPPeerThread>(NUM_PEER);
  List<String> echo = new ArrayList<String>();
  Configuration conf;

  public TestSerializePrinting() {
    this.conf = getConf();
  }

  public void setUp() throws Exception {
    super.setUp();

    ZooKeeper zk = new ZooKeeper("localhost:21810", 3000, this);
    Stat s = null;
    if (zk != null) {
      try {
        s = zk.exists(Constants.DEFAULT_ZOOKEEPER_ROOT, false);
      } catch (Exception e) {
        LOG.error(s);
      }

      if (s == null) {
        try {
          zk.create(Constants.DEFAULT_ZOOKEEPER_ROOT, new byte[0],
              Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          LOG.error(e);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }
    }
  }

  public void testHelloWorld() throws InterruptedException, IOException {
    BSPPeerThread thread;
    int[] randomSequence = new int[] { 2, 3, 4, 5, 0, 1, 6, 7, 8, 9 };
    for (int i = 0; i < NUM_PEER; i++) {
      conf.setInt("bsp.peers.num", NUM_PEER);
      conf.set(Constants.PEER_HOST, "localhost");
      conf.set(Constants.PEER_PORT, String
          .valueOf(30000 + randomSequence[i]));
      conf.set(Constants.ZOOKEEPER_SERVER_ADDRS, "localhost:21810");
      thread = new BSPPeerThread(conf, randomSequence[i]);
      System.out.println(randomSequence[i] + ", " + thread.getName());
      list.add(thread);
    }

    for (int i = 0; i < NUM_PEER; i++) {
      list.get(i).start();
    }

    for (int i = 0; i < NUM_PEER; i++) {
      list.get(i).join();
    }
  }

  public class BSPPeerThread extends Thread {
    private BSPPeer peer;
    private int myId;

    public BSPPeerThread(Configuration conf, int myId) throws IOException {
      conf.set(Constants.ZOOKEEPER_QUORUM, "localhost");
      
      this.peer = new BSPPeer(conf);
      this.myId = myId;
    }

    @Override
    public void run() {
      for (int i = 0; i < NUM_PEER; i++) {
        if (myId == i) {
          echo.add(getName());
          System.out.println("Hello BSP from " + i + " of " + NUM_PEER + ": "
              + getName());
        }

        try {
          Thread.sleep(2000);
          peer.sync();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (KeeperException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
    }
  }

  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub

  }
}
