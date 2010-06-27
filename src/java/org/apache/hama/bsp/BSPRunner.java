package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class BSPRunner extends Thread implements Configurable {
  private static final Log LOG = LogFactory.getLog(BSPRunner.class);
  private BSPPeer bspPeer;
  private Configuration conf;
  private BSP bsp;

  public void run() {
    try {
      bsp.bsp(bspPeer);
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.bspPeer = new BSPPeer(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }

    bsp = (BSP) ReflectionUtils.newInstance(conf.getClass("bsp.work.class",
        BSP.class), conf);
  }
}
