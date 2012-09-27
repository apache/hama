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
package org.apache.hama.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hama.bsp.BSPMessageBundle;

public class CompressionUtil {

  /**
   * Calculates the compression ratio. A compression ratio of less than 1 is
   * desirable.
   * 
   * @param compressedSize
   * @param bundleSize
   * @return the compression ratio
   * @throws IOException
   */
  public static float getCompressionRatio(float compressedSize, float bundleSize)
      throws IOException {
    return (compressedSize / bundleSize);
  }

  public static float getBundleSize(BSPMessageBundle<?> bundle)
      throws IOException {
    DataOutputStream dos = new DataOutputStream(new ByteArrayOutputStream());
    bundle.write(dos);
    dos.close();

    return dos.size();
  }
}
