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

import junit.framework.Assert;

/**
 * This class contains some test utilities.
 */
public class UtilTT {
  /**
   * Creates a "record" containing "length" repetitions of the indicated byte.
   */
  public static byte[] makeRecord(int length, byte b) {
    byte[] retval = new byte[length];
    for (int i = 0; i < length; i++)
      retval[i] = b;
    return retval;
  }

  /**
   * Checks whether the record has the indicated length and data
   */
  public static boolean checkRecord(byte[] data, int length, byte b) {
    Assert.assertEquals("lenght does not match", length, data.length);
    for (int i = 0; i < length; i++)
      Assert.assertEquals("byte " + i, b, data[i]);

    return true;
  }

}
