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
import org.apache.log4j.Logger;

/**
 * Provides a bytes utility
 */
public class BytesUtil {
  static final Logger LOG = Logger.getLogger(BytesUtil.class);
  public static final int SIZEOF_DOUBLE = Double.SIZE/Byte.SIZE;
  public static final int PAD_SIZE = 15; 
  
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
   * Gets the row index
   * 
   * @param bytes
   * @return the converted value
   */
  public static int getRowIndex(byte[] bytes) {
    String rKey = Bytes.toString(bytes); //new String(bytes);
    // return zero If all zero
    if(rKey.equals("000000000000000")) {
      return 0;
    }
    
    if(rKey.substring(0, 8).equals("00000000")){
      int i = 8;
      while (rKey.charAt(i) == '0') {
        i++;
      }
      return Integer.parseInt(rKey.substring(i, rKey.length()));
    } else {
      int i = 0;
      while (rKey.charAt(i) == '0') {
        i++;
      }
      return Integer.parseInt(rKey.substring(i, rKey.length()));
    }
  }

  /**
   * Gets the row index
   * 
   * @param integer
   * @return the converted value
   */
  public static byte[] getRowIndex(int integer) {
    String index = String.valueOf(integer);
    int zeros = PAD_SIZE - index.length();
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < zeros; ++i) {
      buf.append("0");
    }
    return Bytes.toBytes(buf.toString() + index);
  }

}
