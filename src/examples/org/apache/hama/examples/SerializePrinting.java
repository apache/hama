package org.apache.hama.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.zookeeper.KeeperException;

public class SerializePrinting {
  
  public static class HelloBSP extends BSP {
    private Configuration conf;

    @Override
    public void bsp(BSPPeer bspPeer) throws IOException, KeeperException,
        InterruptedException {
      int num = Integer.parseInt(conf.get("bsp.peers.num"));

      for (int i = 0; i < num; i++) {
        if (bspPeer.getId() == i) {
          System.out.println("Hello BSP from " + i + " of " + num + ": "
              + bspPeer.getServerName());
        }

        Thread.sleep(100);
        bspPeer.sync();
      }

    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

  }

  public static void main(String[] args) throws InterruptedException,
      IOException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    // Execute locally
    conf.set("bsp.master.address", "local");

    BSPJob bsp = new BSPJob(conf, SerializePrinting.class);
    // Set the job name
    bsp.setJobName("serialize printing");
    bsp.setBspClass(HelloBSP.class);

    bsp.setNumBspTask(5);
    BSPJobClient.runJob(bsp);
  }
}
