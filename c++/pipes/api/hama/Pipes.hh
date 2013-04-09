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
#ifndef HAMA_PIPES_HH
#define HAMA_PIPES_HH

#ifdef SWIG
%module (directors="1") HamaPipes
%include "std_string.i"
%feature("director") BSP;
%feature("director") Partitioner;
%feature("director") RecordReader;
%feature("director") RecordWriter;
%feature("director") Factory;
#else
#include <string>
#include <vector>
#endif

#include <stdint.h>

using std::string;
using std::vector;

namespace HamaPipes {

/**
 * This interface defines the interface between application code and the 
 * foreign code interface to Hadoop Map/Reduce.
 */

/**
 * A BSPJob defines the properties for a job.
 */
class BSPJob {
public:
  virtual bool hasKey(const string& key) const = 0;
  virtual const string& get(const string& key) const = 0;
  virtual int getInt(const string& key) const = 0;
  virtual float getFloat(const string& key) const = 0;
  virtual bool getBoolean(const string&key) const = 0;
  virtual ~BSPJob() {}
};

/**
 * Task context provides the information about the task and job.
 */
class TaskContext {
public:
  /**
   * Counter to keep track of a property and its value.
   */
  class Counter {
  private:
    int id;
  public:
    Counter(int counterId) : id(counterId) {}
    Counter(const Counter& counter) : id(counter.id) {}

    int getId() const { return id; }
  };
  
  /**
   * Get the BSPJob for the current task.
   */
  virtual const BSPJob* getBSPJob() = 0;

  /**
   * Get the current key. 
   * @return the current key
   */
  //virtual const string& getInputKey() = 0;

  /**
   * Get the current value. 
   * @return the current value
   */
  //virtual const string& getInputValue() = 0;

  /**
   * Generate an output record
   */
  //virtual void emit(const string& key, const string& value) = 0;

  /**
   * Mark your task as having made progress without changing the status 
   * message.
   */
  //virtual void progress() = 0;

  /**
   * Set the status message and call progress.
   */
  //virtual void setStatus(const string& status) = 0;

  /**
   * Register a counter with the given group and name.
   */
  //virtual Counter* getCounter(const string& group, const string& name) = 0;

  /**
   * Increment the value of the counter with the given amount.
   */
  //virtual void incrementCounter(const Counter* counter, uint64_t amount) = 0;
  virtual void incrementCounter(const string& group, const string& name, uint64_t amount) = 0;
  
  virtual ~TaskContext() {}
};
    
    
/**
 * SequenceFile Connector
 */
class SequenceFileConnector {
public:
  /**
   * Open SequenceFile with option "r" or "w"
   * key and value type of the values stored in the SequenceFile
   * @return the corresponding fileID
   */
  virtual int sequenceFileOpen(const string& path, const string& option, const string& keyType, const string& valueType) = 0;
    
  /**
   * Read next key/value pair from the SequenceFile with fileID
   */
  virtual bool sequenceFileReadNext(int fileID, string& key, string& value) = 0;

  /**
   * Append the next key/value pair to the SequenceFile with fileID
   */
  virtual bool sequenceFileAppend(int fileID, const string& key, const string& value) = 0;
    
  /**
   * Close SequenceFile
   */
  virtual bool sequenceFileClose(int fileID) = 0;
};    


class BSPContext: public TaskContext, public SequenceFileConnector {
public:

  /**
   * Access the InputSplit of the mapper.
   */
  //virtual const string& getInputSplit() = 0;

  /**
   * Get the name of the key class of the input to this task.
   */
  virtual const string& getInputKeyClass() = 0;

  /**
   * Get the name of the value class of the input to this task.
   */
  virtual const string& getInputValueClass() = 0;
    
    
    
  /**
   * Send a data with a tag to another BSPSlave corresponding to hostname.
   * Messages sent by this method are not guaranteed to be received in a sent
   * order.
   */
  virtual void sendMessage(const string& peerName, const string& msg) = 0;
    
  /**
   * @return A message from the peer's received messages queue (a FIFO).
   */
  virtual const string& getCurrentMessage() = 0;
    
  /**
   * @return The number of messages in the peer's received messages queue.
   */
  virtual int getNumCurrentMessages() = 0;
    
  /**
   * Barrier Synchronization.
   * 
   * Sends all the messages in the outgoing message queues to the corresponding
   * remote peers.
   */
  virtual void sync() = 0;
    
  /**
   * @return the count of current super-step
   */
  virtual long getSuperstepCount() = 0;
     
  /**
   * @return the name of this peer in the format "hostname:port".
   */ 
  virtual const string& getPeerName() = 0;
    
  /**
   * @return the name of n-th peer from sorted array by name.
   */
  virtual const string& getPeerName(int index) = 0;
    
  /**
   * @return the index of this peer from sorted array by name.
   */
  virtual int getPeerIndex() = 0;
    
  /**
   * @return the names of all the peers executing tasks from the same job
   *         (including this peer).
   */
  virtual vector<string> getAllPeerNames() = 0;
    
  /**
   * @return the number of peers
   */
  virtual int getNumPeers() = 0;
    
  /**
   * Clears all queues entries.
   */
  virtual void clear() = 0;
    
  /**
   * Writes a key/value pair to the output collector
   */
  virtual void write(const string& key, const string& value) = 0;
    
  /**
   * Deserializes the next input key value into the given objects;
   */
  virtual bool readNext(string& key, string& value) = 0;
    
    /**
     * Reads the next key value pair and returns it as a pair. It may reuse a
     * {@link KeyValuePair} instance to save garbage collection time.
     * 
     * @return null if there are no records left.
     * @throws IOException
     */
    //public KeyValuePair<K1, V1> readNext() throws IOException;
    
  /**
   * Closes the input and opens it right away, so that the file pointer is at
   * the beginning again.
   */
  virtual void reopenInput() = 0;
    
};
    
class Closable {
public:
  virtual void close() {}
  virtual ~Closable() {}
};

/**
 * The application's BSP class to do bsp.
 */
class BSP: public Closable {
public:
  /**
   * This method is called before the BSP method. It can be used for setup
   * purposes.
   */
  virtual void setup(BSPContext& context) = 0;
   
  /**
   * This method is your computation method, the main work of your BSP should be
   * done here.
   */
  virtual void bsp(BSPContext& context) = 0;
    
  /**
   * This method is called after the BSP method. It can be used for cleanup
   * purposes. Cleanup is guranteed to be called after the BSP runs, even in
   * case of exceptions.
   */
  virtual void cleanup(BSPContext& context) = 0;
};

/**
 * User code to decide where each key should be sent.
 */
class Partitioner {
public:
    
    virtual int partition(const string& key,const string& value, int32_t numTasks) = 0;
    virtual ~Partitioner() {}
};
    
/**
 * For applications that want to read the input directly for the map function
 * they can define RecordReaders in C++.
 */
class RecordReader: public Closable {
public:
  virtual bool next(string& key, string& value) = 0;

  /**
   * The progress of the record reader through the split as a value between
   * 0.0 and 1.0.
   */
  virtual float getProgress() = 0;
};

/**
 * An object to write key/value pairs as they are emited from the reduce.
 */
class RecordWriter: public Closable {
public:
  virtual void emit(const string& key,
                    const string& value) = 0;
};

/**
 * A factory to create the necessary application objects.
 */
class Factory {
public:
  virtual BSP* createBSP(BSPContext& context) const = 0;

  /**
   * Create an application partitioner object.
   * @return the new partitioner or NULL, if the default partitioner should be 
   * used.
   */
  virtual Partitioner* createPartitioner(BSPContext& context) const {
    return NULL;
  }
    
  /**
   * Create an application record reader.
   * @return the new RecordReader or NULL, if the Java RecordReader should be
   *    used.
   */
  virtual RecordReader* createRecordReader(BSPContext& context) const {
    return NULL; 
  }

  /**
   * Create an application record writer.
   * @return the new RecordWriter or NULL, if the Java RecordWriter should be
   *    used.
   */
  virtual RecordWriter* createRecordWriter(BSPContext& context) const {
    return NULL;
  }

  virtual ~Factory() {}
};

/**
 * Run the assigned task in the framework.
 * The user's main function should set the various functions using the 
 * set* functions above and then call this.
 * @return true, if the task succeeded.
 */
bool runTask(const Factory& factory);

}

#endif
