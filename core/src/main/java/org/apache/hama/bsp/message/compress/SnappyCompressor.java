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

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class SnappyCompressor<M extends Writable> implements
    BSPMessageCompressor<M> {

  public BSPCompressedBundle compressBundle(BSPMessageBundle<M> bundle) {
    BSPCompressedBundle compMsgBundle = null;
    ByteArrayOutputStream bos = null;
    SnappyOutputStream sos = null;
    DataOutputStream dos = null;

    try {
      bos = new ByteArrayOutputStream();
      sos = new SnappyOutputStream(bos);
      dos = new DataOutputStream(sos);

      bundle.write(dos);
      dos.close(); // Flush the stream as no more data will be sent.

      byte[] data = bos.toByteArray();
      compMsgBundle = new BSPCompressedBundle(data);

    } catch (IOException ioe) {
      LOG.error("Unable to compress", ioe);
    } finally {
      try {
        sos.close();
        bos.close();
      } catch (IOException e) {
        LOG.warn("Failed to close compression streams.", e);
      }
    }
    return compMsgBundle;
  }

  /**
   * Decompresses a BSPCompressedBundle and returns the corresponding
   * BSPMessageBundle.
   * 
   * @param compMsgBundle
   * @return
   */
  public BSPMessageBundle<M> decompressBundle(BSPCompressedBundle compMsgBundle) {
    ByteArrayInputStream bis = null;
    SnappyInputStream sis = null;
    DataInputStream dis = null;
    BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();

    try {
      byte[] data = compMsgBundle.getData();
      bis = new ByteArrayInputStream(data);
      sis = new SnappyInputStream(bis);
      dis = new DataInputStream(sis);

      bundle.readFields(dis);

    } catch (IOException ioe) {
      LOG.error("Unable to decompress.", ioe);
    } finally {
      try {
        dis.close();
        sis.close();
        bis.close();
      } catch (IOException e) {
        LOG.warn("Failed to close decompression streams.", e);
      }
    }

    return bundle;
  }

}
