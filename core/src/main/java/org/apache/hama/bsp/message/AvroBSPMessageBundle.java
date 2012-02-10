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
package org.apache.hama.bsp.message;

import java.nio.ByteBuffer;

import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.Writable;

public final class AvroBSPMessageBundle<M extends Writable> extends SpecificRecordBase implements
    SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema
      .parse("{\"type\":\"record\",\"name\":\"AvroBSPMessage\",\"namespace\":\"de.jungblut.avro\",\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}");
  @Deprecated
  public java.nio.ByteBuffer data;

  public final org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  public final java.lang.Object get(int field$) {
    switch (field$) {
      case 0:
        return data;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader. Applications should not call.
  public final void put(int field$, java.lang.Object value$) {
    switch (field$) {
      case 0:
        data = (java.nio.ByteBuffer) value$;
        break;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'data' field.
   */
  public final java.nio.ByteBuffer getData() {
    return data;
  }

  /**
   * Sets the value of the 'data' field.
   * 
   * @param value the value to set.
   */
  public final void setData(java.nio.ByteBuffer value) {
    this.data = value;
  }

  /** Creates a new AvroBSPMessage RecordBuilder */
  public final static AvroBSPMessageBundle.Builder newBuilder() {
    return new AvroBSPMessageBundle.Builder();
  }

  /** Creates a new AvroBSPMessage RecordBuilder by copying an existing Builder */
  public final static AvroBSPMessageBundle.Builder newBuilder(
      AvroBSPMessageBundle.Builder other) {
    return new AvroBSPMessageBundle.Builder(other);
  }

  /**
   * Creates a new AvroBSPMessage RecordBuilder by copying an existing
   * AvroBSPMessage instance
   */
  public final static AvroBSPMessageBundle.Builder newBuilder(
      AvroBSPMessageBundle other) {
    return new AvroBSPMessageBundle.Builder(other);
  }

  /**
   * RecordBuilder for AvroBSPMessage instances.
   */
  public final static class Builder extends
      org.apache.avro.specific.SpecificRecordBuilderBase<AvroBSPMessageBundle>
      implements org.apache.avro.data.RecordBuilder<AvroBSPMessageBundle> {

    private java.nio.ByteBuffer data;

    /** Creates a new Builder */
    private Builder() {
      super(AvroBSPMessageBundle.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(AvroBSPMessageBundle.Builder other) {
      super(other);
    }

    /** Creates a Builder by copying an existing AvroBSPMessage instance */
    private Builder(AvroBSPMessageBundle other) {
      super(AvroBSPMessageBundle.SCHEMA$);
      if (isValidValue(fields[0], other.data)) {
        data = (java.nio.ByteBuffer) clone(other.data);
        fieldSetFlags[0] = true;
      }
    }

    public final ByteBuffer clone(ByteBuffer original) {
      ByteBuffer clone = ByteBuffer.allocate(original.capacity());
      original.rewind();
      clone.put(original);
      original.rewind();
      clone.flip();
      return clone;
    }

    /** Gets the value of the 'data' field */
    public final java.nio.ByteBuffer getData() {
      return data;
    }

    /** Sets the value of the 'data' field */
    public final AvroBSPMessageBundle.Builder setData(java.nio.ByteBuffer value) {
      validate(fields[0], value);
      this.data = value;
      fieldSetFlags[0] = true;
      return this;
    }

    /** Checks whether the 'data' field has been set */
    public final boolean hasData() {
      return fieldSetFlags[0];
    }

    /** Clears the value of the 'data' field */
    public final AvroBSPMessageBundle.Builder clearData() {
      data = null;
      fieldSetFlags[0] = false;
      return this;
    }

    @Override
    public final AvroBSPMessageBundle build() {
      try {
        AvroBSPMessageBundle record = new AvroBSPMessageBundle();
        record.data = fieldSetFlags[0] ? this.data
            : (java.nio.ByteBuffer) getDefaultValue(fields[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
