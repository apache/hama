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
import java.io.IOError;
import java.io.IOException;

/**
 * An record lazily loaded from store. This is used in BTree/HTree to store big
 * records outside of index tree
 */
public final class BTreeLazyRecord<E> {

  /**
   * if value in tree is serialized in more bytes, it is stored as separate
   * record outside of tree This value must be always smaller than 250
   */
  static final int MAX_INTREE_RECORD_SIZE = 32;
  static final int NULL = 255;
  static final int LAZY_RECORD = 254;

  private E value = null;
  private DBAbstract db;
  private Serializer<E> serializer;
  final long recid;

  BTreeLazyRecord(DBAbstract db, long recid, Serializer<E> serializer) {
    this.db = db;
    this.recid = recid;
    this.serializer = serializer;
  }

  E get() {
    if (value != null)
      return value;
    try {
      value = db.fetch(recid, serializer);
    } catch (IOException e) {
      throw new IOError(e);
    }
    return value;
  }

  void delete() {
    try {
      db.delete(recid);
    } catch (IOException e) {
      throw new IOError(e);
    }
    value = null;
    serializer = null;
    db = null;
  }

  /**
   * Serialier used to insert already serialized data into store
   */
  @SuppressWarnings("rawtypes")
  static final Serializer FAKE_SERIALIZER = new Serializer<Object>() {

    @Override
    public void serialize(DataOutput out, Object obj) throws IOException {
      byte[] data = (byte[]) obj;
      out.write(data);
    }

    @Override
    public Object deserialize(DataInput in) throws IOException,
        ClassNotFoundException {
      throw new UnsupportedOperationException();
    }
  };

  static Object fastDeser(DataInputOutput in, Serializer<?> serializer,
      int expectedSize) throws IOException, ClassNotFoundException {
    // we should propably copy data for deserialization into separate buffer and
    // pass it to Serializer
    // but to make it faster, Serializer will operate directly on top of buffer.
    // and we check that it readed correct number of bytes.
    int origAvail = in.available();
    if (origAvail == 0)
      throw new InternalError(); // is backed up by byte[] buffer, so there
                                 // should be always avail bytes
    Object ret = serializer.deserialize(in);
    // check than valueSerializer did not read more bytes, if yes it readed
    // bytes from next record
    int readed = origAvail - in.available();
    if (readed > expectedSize)
      throw new IOException("Serializer readed more bytes than is record size.");
    else if (readed != expectedSize) {
      // deserializer did not readed all bytes, unussual but valid.
      // Skip some to get into correct position
      for (int ii = 0; ii < expectedSize - readed; ii++)
        in.readUnsignedByte();
    }
    return ret;
  }

}
