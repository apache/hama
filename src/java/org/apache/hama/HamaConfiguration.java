package org.apache.hama;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Adds Hama configuration files to a Configuration
 */
public class HamaConfiguration extends HBaseConfiguration {
  /** constructor */
  public HamaConfiguration() {
    super();
    addHbaseResources();
  }
  
  /**
   * Create a clone of passed configuration.
   * @param c Configuration to clone.
   */
  public HamaConfiguration(final Configuration c) {
    this();
    for (Entry<String, String>e: c) {
      set(e.getKey(), e.getValue());
    }
  }
  
  private void addHbaseResources() {
    addResource("hama-site.xml");
  }
}
