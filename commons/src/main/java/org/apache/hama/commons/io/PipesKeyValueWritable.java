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
package org.apache.hama.commons.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * PipesKeyValueWritable containing a Text key-value WritableComparable pair.
 * Format: delimiter | key | delimiter | value e.g., :Key:Value with delimiter
 * ':' or |Key|Value with delimiter '|'
 */
public final class PipesKeyValueWritable extends KeyValueWritable<Text, Text> {

  // private static final Log LOG =
  // LogFactory.getLog(PipesKeyValueWritable.class);

  /**
   * Delimiter between key and value
   */
  private char keyValueDelimiter;

  public PipesKeyValueWritable() {
    super();
  }

  public PipesKeyValueWritable(Text key, Text value, char keyValueDelimiter) {
    super(key, value);
    this.keyValueDelimiter = keyValueDelimiter;
  }

  public char getKeyValueDelimiter() {
    return keyValueDelimiter;
  }

  public void setKeyValueDelimiter(char keyValueDelimiter) {
    this.keyValueDelimiter = keyValueDelimiter;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String str = Text.readString(in);
    // LOG.debug("readFields: '" + str + "'");

    this.keyValueDelimiter = str.charAt(0);
    str = str.substring(1);
    String[] result = str.split(String.valueOf(this.keyValueDelimiter), 2);
    super.setKey(new Text(result[0]));
    super.setValue(new Text(result[1]));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // LOG.debug("write: '" + this.toString() + "'");
    Text.writeString(out, this.toString());
  }

  @Override
  public String toString() {
    return this.keyValueDelimiter + super.getKey().toString()
        + this.keyValueDelimiter + super.getValue().toString();
  }

  @Override
  public int compareTo(KeyValueWritable<Text, Text> obj) {
    // if key is numeric compare numbers
    if ((isNumeric(key.toString())) && (isNumeric(obj.key.toString()))) {
      double val1 = Double.parseDouble(key.toString());
      double val2 = Double.parseDouble(obj.key.toString());
      int cmp = Double.compare(val1, val2);
      if (cmp != 0) {
        return cmp;
      }
    } else { // else compare key string
      int cmp = key.compareTo(obj.key);
      if (cmp != 0) {
        return cmp;
      }
    }
    // if keys are equal compare value
    return value.compareTo(obj.value);
  }

  public final static boolean isNumeric(String s) {
    return s.matches("[-+]?\\d*\\.?\\d+");
  }
}
