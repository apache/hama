/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.Constants;

/**
 * Provides a number format conversion
 */
public class Numeric {
  private static long BITMASK = 0xFF;

  /**
   * Bytes to integer conversion
   * 
   * @param bytes
   * @return the converted value
   */
  public static int bytesToInt(byte[] bytes) {
    return Integer.parseInt(Bytes.toString(bytes));
  }

  /**
   * Integer to bytes conversion
   * 
   * @param integer
   * @return the converted value
   */
  public static byte[] intToBytes(int integer) {
    return Bytes.toBytes(String.valueOf(integer));
  }

  /**
   * Bytes to double conversion
   * 
   * @param bytes
   * @return the converted value
   */
  public static double bytesToDouble(byte[] bytes) {
    long value = 0;
    for (int i = 0; i < 8; i++)
      value = ((((long)(bytes[i])) & 0xFF) << (56 - i * 8)) | value;
    return Double.longBitsToDouble(value);
  }

  /**
   * Double to bytes conversion
   * 
   * @param doubleValue
   * @return the converted value
   */
  public static byte[] doubleToBytes(Double doubleValue) {
    byte[] buf = new byte[8];
    long longVal = Double.doubleToLongBits(doubleValue);
    buf[0] = (Long.valueOf((longVal & (BITMASK << 56)) >>> 56)).byteValue();
    buf[1] = (Long.valueOf((longVal & (BITMASK << 48)) >>> 48)).byteValue();
    buf[2] = (Long.valueOf((longVal & (BITMASK << 40)) >>> 40)).byteValue();
    buf[3] = (Long.valueOf((longVal & (BITMASK << 32)) >>> 32)).byteValue();
    buf[4] = (Long.valueOf((longVal & (long)0x00000000FF000000) >>> 24)).byteValue();
    buf[5] = (Long.valueOf((longVal & (long)0x0000000000FF0000) >>> 16)).byteValue();
    buf[6] = (Long.valueOf((longVal & (long)0x000000000000FF00) >>>  8)).byteValue();
    buf[7] = (Long.valueOf((longVal & (long)0x00000000000000FF))).byteValue();
    return buf;
  }

  /**
   * Gets the column index
   * 
   * @param bytes
   * @return the converted value
   */
  public static int getColumnIndex(byte[] bytes) {
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
  public static byte[] getColumnIndex(int integer) {
    return Bytes.toBytes(Constants.COLUMN + String.valueOf(integer));
  }
}
