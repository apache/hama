package org.apache.hama;

public abstract class AbstractBase {
  /**
   * Return the integer column index
   * 
   * @param b key
   * @return integer
   */
  public int getColumnIndex(byte[] b) {
    String cKey = new String(b);
    return Integer.parseInt(cKey
        .substring(cKey.indexOf(":") + 1, cKey.length()));
  }
}
