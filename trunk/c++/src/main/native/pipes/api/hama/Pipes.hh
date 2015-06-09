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

#include <errno.h>
#include <map>
#include <netinet/in.h>
#include <pthread.h>
#include <sstream> /* ostringstream */
#include <stdint.h>
#include <stdio.h> /* printf */
#include <stdlib.h> /* getenv */
#include <string>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <typeinfo> /* typeid */
#include <unistd.h> /* sleep */
#include <vector>

#include "hadoop/SerialUtils.hh"
#include "hadoop/StringUtils.hh"

#define stringify( name ) # name

using std::map;
using std::string;
using std::vector;
using std::pair;

namespace HamaPipes {
  
  // global varibales
  bool logging;
  
  /********************************************/
  /*************** MESSAGE_TYPE ***************/
  /********************************************/
  enum MESSAGE_TYPE {
    START_MESSAGE, SET_BSPJOB_CONF, SET_INPUT_TYPES,
    RUN_SETUP, RUN_BSP, RUN_CLEANUP,
    READ_KEYVALUE, WRITE_KEYVALUE,
    GET_MSG, GET_MSG_COUNT,
    SEND_MSG, SYNC,
    GET_ALL_PEERNAME, GET_PEERNAME,
    GET_PEER_INDEX, GET_PEER_COUNT, GET_SUPERSTEP_COUNT,
    REOPEN_INPUT, CLEAR,
    CLOSE, ABORT,
    DONE, TASK_DONE,
    REGISTER_COUNTER, INCREMENT_COUNTER,
    SEQFILE_OPEN, SEQFILE_READNEXT,
    SEQFILE_APPEND, SEQFILE_CLOSE,
    PARTITION_REQUEST, PARTITION_RESPONSE,
    LOG, END_OF_DATA
  };
  
  /* Only needed for debugging output */
  const char* messageTypeNames[] = {
    stringify( START_MESSAGE ), stringify( SET_BSPJOB_CONF ), stringify( SET_INPUT_TYPES ),
    stringify( RUN_SETUP ), stringify( RUN_BSP ), stringify( RUN_CLEANUP ),
    stringify( READ_KEYVALUE ), stringify( WRITE_KEYVALUE ),
    stringify( GET_MSG ), stringify( GET_MSG_COUNT ),
    stringify( SEND_MSG ), stringify( SYNC ),
    stringify( GET_ALL_PEERNAME ), stringify( GET_PEERNAME ),
    stringify( GET_PEER_INDEX ), stringify( GET_PEER_COUNT ), stringify( GET_SUPERSTEP_COUNT ),
    stringify( REOPEN_INPUT ), stringify( CLEAR ),
    stringify( CLOSE ), stringify( ABORT ),
    stringify( DONE ), stringify( TASK_DONE ),
    stringify( REGISTER_COUNTER ), stringify( INCREMENT_COUNTER ),
    stringify( SEQFILE_OPEN ), stringify( SEQFILE_READNEXT ),
    stringify( SEQFILE_APPEND ), stringify( SEQFILE_CLOSE ),
    stringify( PARTITION_REQUEST ), stringify( PARTITION_RESPONSE ),
    stringify( LOG ), stringify( END_OF_DATA )
  };
  
  /********************************************/
  /*************** KeyValuePair ***************/
  /********************************************/
  /**
   * Generic KeyValuePair including is_empty
   */
  template <typename K, typename V>
  struct KeyValuePair : pair<K, V> {
    typedef pair<K, V> base_t;
    bool is_empty;
    
    KeyValuePair() : is_empty(false) {}
    explicit KeyValuePair(bool x) : is_empty(x) {}
    KeyValuePair(const K& k, const V& v) : base_t(k, v), is_empty(false) {}
    
    template <typename X, typename Y>
    KeyValuePair(const pair<X,Y> &p) : base_t(p), is_empty(false) {}
    
    template <typename X, typename Y>
    KeyValuePair(const KeyValuePair<X,Y> &p) : base_t(p), is_empty(p.is_empty) {}
  };
  
  /**
   * Override Generic KeyValuePair << operator
   */
  template <typename OS, typename K, typename V>
  OS &operator<<(OS &os, const KeyValuePair<K, V>& p) {
    os << "<KeyValuePair: ";
    if (!p.is_empty) {
      os << p.first << ", " << p.second;
    } else {
      os << "empty";
    }
    os << ">";
    return os;
  }
  
  /********************************************/
  /****************** BSPJob ******************/
  /********************************************/
  /**
   * A BSPJob defines the properties for a job.
   */
  class BSPJob {
  public:
    virtual bool hasKey(const string& key) const = 0;
    virtual const string& get(const string& key) const = 0;
    virtual int getInt(const string& key) const = 0;
    virtual float getFloat(const string& key) const = 0;
    virtual bool getBoolean(const string& key) const = 0;
    virtual ~BSPJob() {}
  };
  
  /********************************************/
  /**************** BSPJobImpl ****************/
  /********************************************/
  class BSPJobImpl: public BSPJob {
  private:
    map<string, string> values_;
  public:
    void set(const string& key, const string& value) {
      values_[key] = value;
    }
    
    virtual bool hasKey(const string& key) const {
      return values_.find(key) != values_.end();
    }
    
    virtual const string& get(const string& key) const {
      map<string,string>::const_iterator itr = values_.find(key);
      if (itr == values_.end()) {
        throw HadoopUtils::Error("Key " + key + " not found in BSPJob");
      }
      return itr->second;
    }
    
    virtual int getInt(const string& key) const {
      const string& val = get(key);
      return HadoopUtils::toInt(val);
    }
    
    virtual float getFloat(const string& key) const {
      const string& val = get(key);
      return HadoopUtils::toFloat(val);
    }
    
    virtual bool getBoolean(const string& key) const {
      const string& val = get(key);
      return HadoopUtils::toBool(val);
    }
  };
  
  /********************************************/
  /**************** TaskContext ***************/
  /********************************************/
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
      int id_;
    public:
      Counter(int counter_id) : id_(counter_id) {}
      Counter(const Counter& counter) : id_(counter.id_) {}
      
      int getId() const { return id_; }
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
    virtual long getCounter(const string& group, const string& name) = 0;
    
    /**
     * Increment the value of the counter with the given amount.
     */
    //virtual void incrementCounter(const Counter* counter, uint64_t amount) = 0;
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) = 0;
    
    virtual ~TaskContext() {}
  };
  
  /********************************************/
  /******** DownwardProtocolPartition *********/
  /********************************************/
  /* DownwardProtocolPartition wraps void template parameter */
  template<typename D, typename K, typename V>
  class DownwardProtocolPartition {
  public:
    void runPartition(const K& key, const V& value, int32_t num_tasks) {
      static_cast<D*>(this)->template runPartition<K,V>(key, value, num_tasks);
    }
  };
  
  template<typename D, typename K>
  class DownwardProtocolPartition<D, K, void> {
  public:
    void runPartition(const K& key, int32_t num_tasks) {
      static_cast<D*>(this)->template runPartition<K>(key, num_tasks);
    }
  };
  
  template<typename D, typename V>
  class DownwardProtocolPartition<D, void, V> {
  public:
    void runPartition(const V& value, int32_t num_tasks) {
      static_cast<D*>(this)->template runPartition<V>(value, num_tasks);
    }
  };
  
  template<typename D>
  class DownwardProtocolPartition<D, void, void> {
  public:
    /* Partition nothing */
  };
  
  /********************************************/
  /************* DownwardProtocol *************/
  /********************************************/
  template<typename D, typename K1, typename V1>
  class DownwardProtocol : public DownwardProtocolPartition<D, K1, V1>{
  public:
    virtual void start(int protocol_version) = 0;
    virtual void setBSPJob(vector<string> values) = 0;
    virtual void setInputTypes(string key_type, string value_type) = 0;
    
    virtual void runBsp() = 0;
    virtual void runCleanup() = 0;
    virtual void runSetup() = 0;
    
    virtual void close() = 0;
    virtual void abort() = 0;
    virtual ~DownwardProtocol() {}
  };
  
  /********************************************/
  /************** UpwardProtocol **************/
  /********************************************/
  template<typename D>
  class UpwardProtocol {
  public:
    virtual void sendCommand(int32_t cmd) = 0;
    
    template<typename T>
    void sendCommand(int32_t cmd, T value) {
      static_cast<D*>(this)->template sendCommand<T>(cmd, value);
    }
    
    template<typename T>
    void sendCommand(int32_t cmd, const T values[], int size) {
      static_cast<D*>(this)->template sendCommand<T>(cmd, values, size);
    }
    
    template<typename T1, typename T2>
    void sendCommand(int32_t cmd, T1 value1, T2 value2) {
      static_cast<D*>(this)->template sendCommand<T1,T2>(cmd, value1, value2);
    }
    
    template<typename T1, typename T2, typename T3>
    void sendCommand(int32_t cmd, T1 value1, T2 value2, T3 value3) {
      static_cast<D*>(this)->template sendCommand<T1,T2,T3>(cmd, value1, value2, value3);
    }
    
    template<typename T1, typename T2>
    void sendCommand(int32_t cmd, T1 value, const T2 values[], int size) {
      static_cast<D*>(this)->template sendCommand<T1,T2>(cmd, value, values, size);
    }
    
    //virtual void registerCounter(int id, const string& group, const string& name) = 0;
    //virtual void incrementCounter(const TaskContext::Counter* counter, uint64_t amount) = 0;
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) = 0;
    virtual ~UpwardProtocol() {}
  };
  
  /********************************************/
  /*********** ProtocolEventHandler ***********/
  /********************************************/
  /* ProtocolEventHandler wraps void template parameter */
  template<typename D, typename K, typename V>
  class ProtocolEventHandler {
  public:
    void nextEvent() {
      static_cast<D*>(this)->template nextEvent<K,V>();
    }
  };
  
  template<typename D, typename K>
  class ProtocolEventHandler<D, K, void> {
  public:
    void nextEvent() {
      static_cast<D*>(this)->template nextEvent<K>();
    }
  };
  
  template<typename D, typename V>
  class ProtocolEventHandler<D, void, V> {
  public:
    void nextEvent() {
      static_cast<D*>(this)->template nextEvent<V>();
    }
  };
  
  template<typename D>
  class ProtocolEventHandler<D, void, void> {
  public:
    void nextEvent() {
      static_cast<D*>(this)->nextEvent();
    }
  };
  
  /********************************************/
  /*********** BinaryUpwardProtocol ***********/
  /********************************************/
  /* Forward definition of BinaryUpwardProtocol to pass to UpwardProtocol */
  class BinaryUpwardProtocol;
  
  /********************************************/
  /***************** Protocol *****************/
  /********************************************/
  template<typename D, typename K1, typename V1>
  class Protocol: public ProtocolEventHandler<D, K1, V1> {
  public:
    
    template<typename T>
    T getResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getResult<T>(expected_response_cmd);
    }
    
    template<typename T>
    vector<T> getVectorResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getVectorResult<T>(expected_response_cmd);
    }
    
    template<typename K, typename V>
    KeyValuePair<K,V> getKeyValueResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getKeyValueResult<K,V>(expected_response_cmd);
    }
    
    template<typename T>
    KeyValuePair<T,T> getKeyValueResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getKeyValueResult<T>(expected_response_cmd);
    }
    
    virtual bool verifyResult(int32_t expected_response_cmd) = 0;
    virtual UpwardProtocol<BinaryUpwardProtocol>* getUplink() = 0;
    virtual ~Protocol(){}
  };
  
  /********************************************/
  /*********** SequenceFileConnector **********/
  /********************************************/
  /**
   * SequenceFile Connector
   */
  /* Using Curiously recurring template pattern(CTRP) */
  /* because virtual template function not possible */
  template<typename D>
  class SequenceFileConnector {
  public:
    /**
     * Open SequenceFile with option "r" or "w"
     * key and value type of the values stored in the SequenceFile
     * @return the corresponding fileID
     */
    virtual int32_t sequenceFileOpen(const string& path, const string& option, const string& key_type, const string& value_type) = 0;
    
    /**
     * Close SequenceFile
     */
    virtual bool sequenceFileClose(int32_t file_id) = 0;
    
    /**
     * Read next key/value pair from the SequenceFile with fileID
     * Using Curiously recurring template pattern(CTRP)
     */
    template<typename K, typename V>
    bool sequenceFileReadNext(int32_t file_id, K& key, V& value) {
      return static_cast<D*>(this)->template sequenceFileReadNext<K,V>(file_id, key, value);
    }
    
    /**
     * Read next key OR value from the SequenceFile with fileID
     * key OR value type is NullWritable
     */
    template<typename T>
    bool sequenceFileReadNext(int32_t file_id, T& key_or_value) {
      return static_cast<D*>(this)->template sequenceFileReadNext<T>(file_id, key_or_value);
    }
    
    /**
     * Append the next key/value pair to the SequenceFile with fileID
     * Using Curiously recurring template pattern(CTRP)
     */
    template<typename K, typename V>
    bool sequenceFileAppend(int32_t file_id, const K& key, const V& value) {
      return static_cast<D*>(this)->template sequenceFileAppend<K,V>(file_id, key, value);
    }
    
    /**
     * Append the next key OR value pair to the SequenceFile with fileID
     * key OR value type is NullWritable
     */
    template<typename T>
    bool sequenceFileAppend(int32_t file_id, const T& key_or_value) {
      return static_cast<D*>(this)->template sequenceFileAppend<T>(file_id, key_or_value);
    }
  };
  
  /********************************************/
  /****************** Reader ******************/
  /********************************************/
  /* Reader wraps void template parameter */
  template<typename D, typename K, typename V>
  class Reader {
  public:
    /**
     * Deserializes the next input key value into the given objects
     */
    bool readNext(K& key, V& value) {
      return static_cast<D*>(this)->template readNext<K,V>(key, value);
    }
    
    /**
     * Reads the next key value pair and returns it as a pair. It may reuse a
     * {@link KeyValuePair} instance to save garbage collection time.
     *
     * @return null if there are no records left.
     * @throws IOException
     */
    //public KeyValuePair<K1, V1> readNext() throws IOException;
  };
  
  template<typename D, typename K>
  class Reader<D, K, void> {
  public:
    /**
     * Deserializes the next input key into the given object
     * value type is NullWritable
     */
    bool readNext(K& key) {
      return static_cast<D*>(this)->template readNext<K>(key);
    }
  };
  
  template<typename D, typename V>
  class Reader<D, void, V> {
  public:
    /**
     * Deserializes the next input value into the given object
     * key type is NullWritable
     */
    bool readNext(V& value) {
      return static_cast<D*>(this)->template readNext<V>(value);
    }
  };
  
  template<typename D>
  class Reader<D, void, void> {
  public:
    /* key AND value type are NullWritable */
    /* Read nothing */
  };
  
  /********************************************/
  /****************** Writer ******************/
  /********************************************/
  /* Writer wraps void template parameter */
  template<typename D, typename K, typename V>
  class Writer {
  public:
    /**
     * Writes a key/value pair to the output collector
     */
    void write(const K& key, const V& value) {
      static_cast<D*>(this)->template write<K,V>(key, value);
    }
  };
  
  template<typename D, typename K>
  class Writer<D, K, void> {
  public:
    /**
     * Writes a key to the output collector
     * value type is NullWritable
     */
    void write(const K& key) {
      static_cast<D*>(this)->template write<K>(key);
    }
  };
  
  template<typename D, typename V>
  class Writer<D, void, V> {
  public:
    /**
     * Writes a value to the output collector
     * key type is NullWritable
     */
    void write(const V& value) {
      static_cast<D*>(this)->template write<V>(value);
    }
  };
  
  template<typename D>
  class Writer<D, void, void> {
  public:
    /* key AND value type are NullWritable */
    /* Write nothing */
  };
  
  /********************************************/
  /**************** Messenger *****************/
  /********************************************/
  /* Messenger wraps void template parameter */
  template<typename D, typename M>
  class Messenger {
  public:
    /**
     * Send a data with a tag to another BSPSlave corresponding to hostname.
     * Messages sent by this method are not guaranteed to be received in a sent
     * order.
     */
    void sendMessage(const string& peer_name, const M& msg) {
      static_cast<D*>(this)->template sendMessage<M>(peer_name, msg);
    }
    
    /**
     * @return A message from the peer's received messages queue (a FIFO).
     */
    virtual M getCurrentMessage() {
      return static_cast<D*>(this)->template getCurrentMessage<M>();
    }
  };
  
  template<typename D>
  class Messenger<D, void> {
  public:
    /* message type is NullWritable */
    /* therefore messenger is not available */
  };
  
  /********************************************/
  /************** BSPContextImpl **************/
  /********************************************/
  /* Forward definition of BSPContextImpl to pass to SequenceFileConnector */
  template<typename K1, typename V1, typename K2, typename V2, typename M>
  class BSPContextImpl;
  
  /********************************************/
  /***************** BSPContext ***************/
  /********************************************/
  template<typename K1, typename V1, typename K2, typename V2, typename M>
  class BSPContext: public TaskContext, public SequenceFileConnector<BSPContextImpl<K1, V1, K2, V2, M> >,
  public Reader<BSPContextImpl<K1, V1, K2, V2, M>, K1, V1>,
  public Writer<BSPContextImpl<K1, V1, K2, V2, M>, K2, V2>,
  public Messenger<BSPContextImpl<K1, V1, K2, V2, M>, M> {
  public:
    
    /**
     * Access the InputSplit of the mapper.
     */
    //virtual const string& getInputSplit() = 0;
    
    /**
     * Get the name of the key class of the input to this task.
     */
    virtual string getInputKeyClass() = 0;
    
    /**
     * Get the name of the value class of the input to this task.
     */
    virtual string getInputValueClass() = 0;
    
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
    virtual string getPeerName() = 0;
    
    /**
     * @return the name of n-th peer from sorted array by name.
     */
    virtual string getPeerName(int index) = 0;
    
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
     * Closes the input and opens it right away, so that the file pointer is at
     * the beginning again.
     */
    virtual void reopenInput() = 0;
  };
  
  /********************************************/
  /****************** Closable ****************/
  /********************************************/
  class Closable {
  public:
    virtual void close() {}
    virtual ~Closable() {}
  };
  
  /********************************************/
  /******************* BSP ********************/
  /********************************************/
  /**
   * The application's BSP class to do bsp.
   */
  template<typename K1, typename V1, typename K2, typename V2, typename M>
  class BSP: public Closable {
  public:
    /**
     * This method is called before the BSP method. It can be used for setup
     * purposes.
     */
    virtual void setup(BSPContext<K1, V1, K2, V2, M>& context) {
      // empty implementation, because overriding is optional
    }
    
    /**
     * This method is your computation method, the main work of your BSP should be
     * done here.
     */
    virtual void bsp(BSPContext<K1, V1, K2, V2, M>& context) = 0;
    
    /**
     * This method is called after the BSP method. It can be used for cleanup
     * purposes. Cleanup is guranteed to be called after the BSP runs, even in
     * case of exceptions.
     */
    virtual void cleanup(BSPContext<K1, V1, K2, V2, M>& context) {
      // empty implementation, because overriding is optional
    }
  };
  
  /********************************************/
  /**************** Partitioner ***************/
  /********************************************/
  /**
   * User code to decide where each key should be sent.
   */
  template<typename K1, typename V1>
  class Partitioner {
  public:
    virtual int partition(const K1& key, const V1& value, int32_t num_tasks) = 0;
    virtual ~Partitioner() {}
  };
  
  template<typename K1>
  class Partitioner<K1, void> {
  public:
    virtual int partition(const K1& key, int32_t num_tasks) = 0;
    virtual ~Partitioner() {}
  };
  
  template<typename V1>
  class Partitioner<void, V1> {
  public:
    virtual int partition(const V1& value, int32_t num_tasks) = 0;
    virtual ~Partitioner() {}
  };
  
  template<>
  class Partitioner<void, void> {
  public:
    /* Partition nothing */
  };
  
  /********************************************/
  /****************** Factory *****************/
  /********************************************/
  /**
   * A factory to create the necessary application objects.
   */
  template<typename K1, typename V1, typename K2, typename V2, typename M>
  class Factory {
  public:
    virtual BSP<K1, V1, K2, V2, M>* createBSP(BSPContext<K1, V1, K2, V2, M>& context) const = 0;
    
    /**
     * Create an application partitioner object.
     * @return the new partitioner or NULL, if the default partitioner should be
     * used.
     */
    virtual Partitioner<K1, V1>* createPartitioner(BSPContext<K1, V1, K2, V2, M>& context) const {
      return NULL;
    }
    
    virtual ~Factory() {}
  };
  
  /********************************************/
  /***************** toString *****************/
  /********************************************/
  /**
   * Generic toString
   */
  template <typename T>
  string toString(const T& t)
  {
    std::ostringstream oss;
    oss << t;
    return oss.str();
  }
  /**
   * Generic toString template specializations
   */
  template <> string toString<string>(const string& t) {
    return t;
  }
  
  /********************************************/
  /*************** Serialization **************/
  /********************************************/
  /**
   * Generic serialization
   */
  template<typename T>
  void serialize(T t, HadoopUtils::OutStream& stream) {
    HadoopUtils::serializeString(toString<T>(t), stream);
  }
  
  /**
   * Generic serialization template specializations
   */
  template <> void serialize<int32_t>(int32_t t, HadoopUtils::OutStream& stream) {
    HadoopUtils::serializeInt(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeInt '%d'\n", t);
    }
  }
  template <> void serialize<int64_t>(int64_t t, HadoopUtils::OutStream& stream) {
    HadoopUtils::serializeLong(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeLong '%ld'\n", (long)t);
    }
  }
  template <> void serialize<float>(float t, HadoopUtils::OutStream& stream) {
    HadoopUtils::serializeFloat(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeFloat '%f'\n", t);
    }
  }
  template <> void serialize<double>(double t, HadoopUtils::OutStream& stream) {
    HadoopUtils::serializeDouble(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeDouble '%f'\n", t);
    }
  }
  template <> void serialize<string>(string t, HadoopUtils::OutStream& stream) {
    HadoopUtils::serializeString(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeString '%s'\n", t.c_str());
    }
  }
  
  /********************************************/
  /************** Deserialization *************/
  /********************************************/
  /**
   * Generic deserialization
   */
  template<typename T>
  T deserialize(HadoopUtils::InStream& stream) {
    string str = "Not able to deserialize type: ";
    throw HadoopUtils::Error(str.append(typeid(T).name()));
  }
  
  /**
   * Generic deserialization template specializations
   */
  template <> int32_t deserialize<int32_t>(HadoopUtils::InStream& stream) {
    int32_t result = HadoopUtils::deserializeInt(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeInt result: '%d'\n",
              result);
    }
    return result;
  }
  template <> int64_t deserialize<int64_t>(HadoopUtils::InStream& stream) {
    int64_t result = HadoopUtils::deserializeLong(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeLong result: '%ld'\n",
              (long)result);
    }
    return result;
  }
  template <> float deserialize<float>(HadoopUtils::InStream& stream) {
    float result = HadoopUtils::deserializeFloat(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeFloat result: '%f'\n",
              result);
    }
    return result;
  }
  template <> double deserialize<double>(HadoopUtils::InStream& stream) {
    double result = HadoopUtils::deserializeDouble(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeDouble result: '%f'\n",
              result);
    }
    return result;
  }
  template <> string deserialize<string>(HadoopUtils::InStream& stream) {
    string result = HadoopUtils::deserializeString(stream);
    
    if (logging) {
      if (result.empty()) {
        fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeString returns EMPTY string! Maybe wrong Serialization?\n");
      } else {
        fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeString result: '%s'\n",
                ((result.length()<10)?result.c_str():result.substr(0,9).append("...").c_str()));
      }
    }
    return result;
  }
  
  /********************************************/
  /*********** runTask entry method ***********/
  /********************************************/
  /**
   * Run the assigned task in the framework.
   * The user's main function should set the various functions using the
   * set* functions above and then call this.
   * @return true, if the task succeeded.
   */
  template<typename K1, typename V1, typename K2, typename V2, typename M>
  bool runTask(const Factory<K1, V1, K2, V2, M>& factory);
  
  // Include implementation in header because of templates
#include "../../impl/Pipes.cc"
}

#endif
