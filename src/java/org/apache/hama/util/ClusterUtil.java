package org.apache.hama.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.bsp.GroomServer;

public class ClusterUtil {
  public static final Log LOG = LogFactory.getLog(ClusterUtil.class);

  /**
   * Data Structure to hold GroomServer Thread and GroomServer instance
   */
  public static class GroomServerThread extends Thread {
    private final GroomServer groomServer;

    public GroomServerThread(final GroomServer r, final int index) {
      super(r, "GroomServer:" + index);
      this.groomServer = r;
    }

    /** @return the groom server */
    public GroomServer getGroomServer() {
      return this.groomServer;
    }

    /**
     * Block until the groom server has come online, indicating it is ready
     * to be used.
     */
    public void waitForServerOnline() {
      while (!groomServer.isRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // continue waiting
        }
      }
    }
  }

  /**
   * Creates a {@link GroomServerThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hrsc Class to create.
   * @param index Used distingushing the object returned.
   * @throws IOException
   * @return Groom server added.
   */
  public static ClusterUtil.GroomServerThread createGroomServerThread(final Configuration c,
    final Class<? extends GroomServer> hrsc, final int index)
  throws IOException {
    GroomServer server;
      try {
        server = hrsc.getConstructor(Configuration.class).newInstance(c);
      } catch (Exception e) {
        IOException ioe = new IOException();
        ioe.initCause(e);
        throw ioe;
      }
      return new ClusterUtil.GroomServerThread(server, index);
  }

  /**
   * Start the cluster.
   * @param m
   * @param conf 
   * @param groomServers
   * @return Address to use contacting master.
   * @throws InterruptedException 
   * @throws IOException 
   */
  public static String startup(final BSPMaster m,
      final List<ClusterUtil.GroomServerThread> groomservers, Configuration conf) throws IOException, InterruptedException {
    if (m != null) {
      m.start();
    }

    if (groomservers != null) {
      for (ClusterUtil.GroomServerThread t: groomservers) {
        t.start();
      }
    }
    
    return m == null? null: BSPMaster.getAddress(conf).getHostName();
  }

  public static void shutdown(BSPMaster master,
      List<GroomServerThread> groomThreads, Configuration conf) {
    LOG.debug("Shutting down HAMA Cluster");
    // TODO: 
  }
}
