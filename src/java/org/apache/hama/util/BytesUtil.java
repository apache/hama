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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.Constants;
import org.apache.hama.SubMatrix;

/**
 * Provides a bytes utility
 */
public class BytesUtil {
  public static final int SIZEOF_DOUBLE = Double.SIZE/Byte.SIZE;
  
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
    return ByteBuffer.wrap(bytes).getDouble();
  }

  /**
   * Double to bytes conversion
   * 
   * @param value
   * @return the converted value
   */
  public static byte[] doubleToBytes(Double value) {
    // If HBASE-884 done, we should change it.
    return ByteBuffer.allocate(SIZEOF_DOUBLE).putDouble(value).array();
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
  
  public static byte[] subMatrixToBytes(Object obj) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(obj);
    oos.flush();
    oos.close();
    bos.close();
    byte[] data = bos.toByteArray();
    return data;
  }
  
  public static SubMatrix bytesToSubMatrix(byte[] value) throws IOException,
      ClassNotFoundException {
    ByteArrayInputStream bos = new ByteArrayInputStream(value);
    ObjectInputStream oos = new ObjectInputStream(bos);
    Object obj = oos.readObject();
    oos.close();
    bos.close();
    return (SubMatrix) obj;
  }
}
