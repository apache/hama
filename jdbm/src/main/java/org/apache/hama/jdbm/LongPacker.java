/**
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
package org.apache.hama.jdbm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Packing utility for non-negative <code>long</code> and values.
 * <p/>
 * Originally developed for Kryo by Nathan Sweet. Modified for JDBM by Jan Kotek
 */
public final class LongPacker {

  /**
   * Pack non-negative long into output stream. It will occupy 1-10 bytes
   * depending on value (lower values occupy smaller space)
   * 
   * @param os
   * @param value
   * @throws IOException
   */
  public static void packLong(DataOutput os, long value) throws IOException {

    if (value < 0) {
      throw new IllegalArgumentException("negative value: v=" + value);
    }

    while ((value & ~0x7FL) != 0) {
      os.write((((int) value & 0x7F) | 0x80));
      value >>>= 7;
    }
    os.write((byte) value);
  }

  /**
   * Unpack positive long value from the input stream.
   * 
   * @param is The input stream.
   * @return The long value.
   * @throws java.io.IOException
   */
  public static long unpackLong(DataInput is) throws IOException {

    long result = 0;
    for (int offset = 0; offset < 64; offset += 7) {
      long b = is.readUnsignedByte();
      result |= (b & 0x7F) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new Error("Malformed long.");
  }

  /**
   * Pack non-negative long into output stream. It will occupy 1-5 bytes
   * depending on value (lower values occupy smaller space)
   * 
   * @param os
   * @param value
   * @throws IOException
   */
  public static void packInt(DataOutput os, int value) throws IOException {

    if (value < 0) {
      throw new IllegalArgumentException("negative value: v=" + value);
    }

    while ((value & ~0x7F) != 0) {
      os.write(((value & 0x7F) | 0x80));
      value >>>= 7;
    }

    os.write((byte) value);
  }

  public static int unpackInt(DataInput is) throws IOException {
    for (int offset = 0, result = 0; offset < 32; offset += 7) {
      int b = is.readUnsignedByte();
      result |= (b & 0x7F) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new Error("Malformed integer.");

  }

}
