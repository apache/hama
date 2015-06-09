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
package org.apache.hama.bsp.message.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;

public class Bzip2Compressor<M extends Writable> extends
    BSPMessageCompressor<M> {

  @Override
  public byte[] compress(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    DataInputStream in = new DataInputStream(bis);

    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outBuffer);

    CompressorOutputStream cos = null;
    try {
      cos = new CompressorStreamFactory().createCompressorOutputStream("bzip2",
          out);
      IOUtils.copy(in, cos);
      cos.close();
    } catch (CompressorException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return outBuffer.toByteArray();
  }

  /**
   * Decompresses a BSPCompressedBundle and returns the corresponding
   * BSPMessageBundle.
   * 
   * @param compressedBytes
   * @return The result after decompressing BSPMessageBundle.
   */
  @Override
  public byte[] decompress(byte[] compressedBytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(compressedBytes);
    DataInputStream in = new DataInputStream(bis);

    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outBuffer);
    try {

      final CompressorInputStream cin = new CompressorStreamFactory()
          .createCompressorInputStream("bzip2", in);
      IOUtils.copy(cin, out);
      in.close();
    } catch (CompressorException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outBuffer.toByteArray();
  }

}
