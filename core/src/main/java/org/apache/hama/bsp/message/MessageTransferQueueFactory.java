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

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.message.queue.MemoryQueue;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.message.queue.MessageTransferProtocol;
import org.apache.hama.util.ReflectionUtils;

/**
 * Factory class to define protocols between the sender and receiver queues.
 * 
 * @param <M> The message type.
 */
public class MessageTransferQueueFactory<M> {

  private static class DefaultMessageTransferQueue<M> implements
      MessageTransferProtocol<M> {

    @SuppressWarnings("unchecked")
    @Override
    public MessageQueue<M> getSenderQueue(Configuration conf) {
      return ReflectionUtils.newInstance(conf.getClass(
          MessageManager.SENDER_QUEUE_TYPE_CLASS, MemoryQueue.class,
          MessageQueue.class));
    }

    @SuppressWarnings("unchecked")
    @Override
    public MessageQueue<M> getReceiverQueue(Configuration conf) {
      return ReflectionUtils.newInstance(conf.getClass(
          MessageManager.RECEIVE_QUEUE_TYPE_CLASS, MemoryQueue.class,
          MessageQueue.class));
    }

  }

  @SuppressWarnings("rawtypes")
  public static MessageTransferProtocol getMessageTransferQueue(Configuration conf) {
    return (MessageTransferProtocol) ReflectionUtils.newInstance(conf.getClass(
        MessageManager.TRANSFER_QUEUE_TYPE_CLASS,
        DefaultMessageTransferQueue.class, MessageTransferProtocol.class));

  }
}
