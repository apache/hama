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
package org.apache.hama.bsp.message.type;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.Messagable;

/**
 * BSPMessage consists of the tag and the arbitrary amount of data to be
 * communicated.
 */
public abstract class BSPMessage implements Messagable, Writable {

  public BSPMessage() {
  }

  /**
   * BSP messages are typically identified with tags. This allows to get the tag
   * of data.
   * 
   * @return tag of data of BSP message
   */
  public abstract Object getTag();

  /**
   * @return data of BSP message
   */
  public abstract Object getData();

  public abstract void setTag(Object tag);

  public abstract void setData(Object data);

}
