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
package org.apache.hama.mapred;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hama.Constants;

public class MatrixMapReduce extends MapReduceBase {

  /**
   * Converts the double to bytes
   * 
   * @param doubleVal
   * @return bytes
   */
  public byte[] toBytes(double doubleVal) {
    byte[] dByteArray = new byte[8];
    long n = Double.doubleToLongBits(doubleVal);

    for (int i = 0; i < dByteArray.length; i++) {
      dByteArray[i] = (byte) (n >> (i * 8) & 0xFF);
    }

    return dByteArray;
  }

  /**
   * Converts the bytes to double
   * 
   * @param inBytes
   * @return double
   */
  public double toDouble(byte[] inBytes) {
    if (inBytes == null) {
      return 0;
    }

    long n = 0;
    for (int i = 0; i < inBytes.length; i++) {
      n |= ((long) (inBytes[i] & 0377) << (i * 8));
    }

    double doubleValue = Double.longBitsToDouble(n);

    return doubleValue;
  }
  
  /**
   * Converts the int to byte array
   * 
   * @param i
   * @return Byte Array
   */
  public byte[] intToBytes(int i) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.order(ByteOrder.nativeOrder());
    bb.putInt(i);
    return bb.array();
  }

  /**
   * Converts the bytes to int
   * 
   * @param bytes
   * @return Integer value
   */
  public int bytesToInt(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    bb.order(ByteOrder.nativeOrder());
    return bb.getInt();
  }

  /**
   * Converts the double to ImmutableBytesWritable
   * 
   * @param d
   * @return ImmutableBytesWritable
   */
  public ImmutableBytesWritable getBytesWritable(double d) {
    return new ImmutableBytesWritable(toBytes(d));
  }
  
  /**
   * Converts the writable to double
   * 
   * @param value
   * @return double
   */
  public double getDouble(Writable value) {
    return toDouble(((ImmutableBytesWritable) value).get());
  }

  /**
   * Return the integer index
   * 
   * @param key
   * @return integer
   */
  public int getIndex(Text key) {
    String sKey = key.toString();
    return Integer.parseInt(sKey
        .substring(sKey.indexOf(":") + 1, sKey.length()));
  }

  /**
   * Return the column key
   * 
   * @param i
   * @return text
   */
  public Text getColumnText(int i) {
    return new Text(Constants.COLUMN + String.valueOf(i));
  }
}
