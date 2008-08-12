package org.apache.hama;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides a number format conversion
 */
public abstract class AbstractBase {

  /**
   * Bytes to integer conversion
   * 
   * @param bytes
   * @return the converted value
   */
  public int bytesToInt(byte[] bytes) {
    return Integer.parseInt(Bytes.toString(bytes));
  }

  /**
   * Integer to bytes conversion
   * 
   * @param integer
   * @return the converted value
   */
  public byte[] intToBytes(int integer) {
    return Bytes.toBytes(String.valueOf(integer));
  }

  /**
   * Bytes to double conversion
   * 
   * @param bytes
   * @return the converted value
   */
  public double bytesToDouble(byte[] bytes) {
    return Double.parseDouble(Bytes.toString(bytes));
  }

  /**
   * Double to bytes conversion
   * 
   * @param doubleValue
   * @return the converted value
   */
  public byte[] doubleToBytes(Double doubleValue) {
    return Bytes.toBytes(doubleValue.toString());
  }

  /**
   * Gets the column index
   * 
   * @param bytes
   * @return the converted value
   */
  public int getColumnIndex(byte[] bytes) {
    String cKey = new String(bytes);
    return Integer.parseInt(cKey
        .substring(cKey.indexOf(":") + 1, cKey.length()));
  }

  /**
   * Gets the column index
   * 
   * @param integer
   * @return the converted value
   */
  public byte[] getColumnIndex(int integer) {
    return Bytes.toBytes(Constants.COLUMN + String.valueOf(integer));
  }
}
