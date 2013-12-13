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

using namespace HadoopUtils;

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
    
    template <class X, class Y>
    KeyValuePair(const pair<X,Y> &p) : base_t(p), is_empty(false) {}
    
    template <class X, class Y>
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
        throw Error("Key " + key + " not found in BSPJob");
      }
      return itr->second;
    }
    
    virtual int getInt(const string& key) const {
      const string& val = get(key);
      return toInt(val);
    }
    
    virtual float getFloat(const string& key) const {
      const string& val = get(key);
      return toFloat(val);
    }
    
    virtual bool getBoolean(const string& key) const {
      const string& val = get(key);
      return toBool(val);
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
  /************* DownwardProtocol *************/
  /********************************************/
  template<class K1, class V1>
  class DownwardProtocol {
  public:
    virtual void start(int protocol_version) = 0;
    virtual void setBSPJob(vector<string> values) = 0;
    virtual void setInputTypes(string key_type, string value_type) = 0;
    
    virtual void runBsp(bool piped_input, bool piped_output) = 0;
    virtual void runCleanup(bool piped_input, bool piped_output) = 0;
    virtual void runSetup(bool piped_input, bool piped_output) = 0;
    
    virtual void runPartition(const K1& key, const V1& value, int32_t num_tasks) = 0;
    
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
    
    template<class T>
    void sendCommand(int32_t cmd, T value) {
      static_cast<D*>(this)->template sendCommand<T>(cmd, value);
    }
    
    template<class T>
    void sendCommand(int32_t cmd, const T values[], int size) {
      static_cast<D*>(this)->template sendCommand<T>(cmd, values, size);
    }
    
    template<class T1, class T2>
    void sendCommand(int32_t cmd, T1 value1, T2 value2) {
      static_cast<D*>(this)->template sendCommand<T1,T2>(cmd, value1, value2);
    }
    
    template<class T1, class T2>
    void sendCommand(int32_t cmd, T1 value, const T2 values[], int size) {
      static_cast<D*>(this)->template sendCommand<T1,T2>(cmd, value, values, size);
    }
    
    //virtual void registerCounter(int id, const string& group, const string& name) = 0;
    //virtual void incrementCounter(const TaskContext::Counter* counter, uint64_t amount) = 0;
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) = 0;
    virtual ~UpwardProtocol() {}
  };
  
  /* Forward definition of BinaryUpwardProtocol to pass to UpwardProtocol */
  class BinaryUpwardProtocol;
  
  /********************************************/
  /***************** Protocol *****************/
  /********************************************/
  template<typename D>
  class Protocol {
  public:
    
    template<class T>
    T getResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getResult<T>(expected_response_cmd);
    }
    
    template<class T>
    vector<T> getVectorResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getVectorResult<T>(expected_response_cmd);
    }
    
    template<class K, class V>
    KeyValuePair<K,V> getKeyValueResult(int32_t expected_response_cmd) {
      return static_cast<D*>(this)->template getKeyValueResult<K,V>(expected_response_cmd);
    }
    
    virtual void nextEvent() = 0;
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
    template<class K, class V>
    bool sequenceFileReadNext(int32_t file_id, K& key, V& value) {
      return static_cast<D*>(this)->template sequenceFileReadNext<K,V>(file_id, key, value);
    }
    
    /**
     * Append the next key/value pair to the SequenceFile with fileID
     * Using Curiously recurring template pattern(CTRP)
     */
    template<class K, class V>
    bool sequenceFileAppend(int32_t file_id, const K& key, const V& value) {
      return static_cast<D*>(this)->template sequenceFileAppend<K,V>(file_id, key, value);
    }
  };
  
  /* Forward definition of BSPContextImpl to pass to SequenceFileConnector */
  template<class K1, class V1, class K2, class V2, class M>
  class BSPContextImpl;
  
  
  template<class K1, class V1, class K2, class V2, class M>
  class BSPContext: public TaskContext, public SequenceFileConnector<BSPContextImpl<K1, V1, K2, V2, M> > {
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
     * Send a data with a tag to another BSPSlave corresponding to hostname.
     * Messages sent by this method are not guaranteed to be received in a sent
     * order.
     */
    virtual void sendMessage(const string& peer_name, const M& msg) = 0;
    
    /**
     * @return A message from the peer's received messages queue (a FIFO).
     */
    virtual M getCurrentMessage() = 0;
    
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
     * Writes a key/value pair to the output collector
     */
    virtual void write(const K2& key, const V2& value) = 0;
    
    /**
     * Deserializes the next input key value into the given objects;
     */
    virtual bool readNext(K1& key, V1& value) = 0;
    
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
  template<class K1, class V1, class K2, class V2, class M>
  class BSP: public Closable {
  public:
    /**
     * This method is called before the BSP method. It can be used for setup
     * purposes.
     */
    virtual void setup(BSPContext<K1, V1, K2, V2, M>& context) = 0;
    
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
    virtual void cleanup(BSPContext<K1, V1, K2, V2, M>& context) = 0;
  };
  
  /**
   * User code to decide where each key should be sent.
   */
  template<class K1, class V1, class K2, class V2, class M>
  class Partitioner {
  public:
    
    virtual int partition(const K1& key, const V1& value, int32_t num_tasks) = 0;
    virtual ~Partitioner() {}
  };
  
  /**
   * For applications that want to read the input directly for the map function
   * they can define RecordReaders in C++.
   */
  template<class K, class V>
  class RecordReader: public Closable {
  public:
    virtual bool next(K& key, V& value) = 0;
    
    /**
     * The progress of the record reader through the split as a value between
     * 0.0 and 1.0.
     */
    virtual float getProgress() = 0;
  };
  
  /**
   * An object to write key/value pairs as they are emited from the reduce.
   */
  template<class K, class V>
  class RecordWriter: public Closable {
  public:
    virtual void emit(const K& key, const V& value) = 0;
  };
  
  /**
   * A factory to create the necessary application objects.
   */
  template<class K1, class V1, class K2, class V2, class M>
  class Factory {
  public:
    virtual BSP<K1, V1, K2, V2, M>* createBSP(BSPContext<K1, V1, K2, V2, M>& context) const = 0;
    
    /**
     * Create an application partitioner object.
     * @return the new partitioner or NULL, if the default partitioner should be
     * used.
     */
    virtual Partitioner<K1, V1, K2, V2, M>* createPartitioner(BSPContext<K1, V1, K2, V2, M>& context) const {
      return NULL;
    }
    
    /**
     * Create an application record reader.
     * @return the new RecordReader or NULL, if the Java RecordReader should be
     *    used.
     */
    virtual RecordReader<K1,V1>* createRecordReader(BSPContext<K1, V1, K2, V2, M>& context) const {
      return NULL;
    }
    
    /**
     * Create an application record writer.
     * @return the new RecordWriter or NULL, if the Java RecordWriter should be
     *    used.
     */
    virtual RecordWriter<K2,V2>* createRecordWriter(BSPContext<K1, V1, K2, V2, M>& context) const {
      return NULL;
    }
    
    virtual ~Factory() {}
  };
  
  /**
   * Generic toString
   */
  template <class T>
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
  
  /**
   * Generic serialization
   */
  template<class T>
  void serialize(T t, OutStream& stream) {
    serializeString(toString<T>(t), stream);
  }
  
  /**
   * Generic serialization template specializations
   */
  template <> void serialize<int32_t>(int32_t t, OutStream& stream) {
    serializeInt(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeInt '%d'\n", t);
    }
  }
  template <> void serialize<int64_t>(int64_t t, OutStream& stream) {
    serializeLong(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeLong '%ld'\n", (long)t);
    }
  }
  template <> void serialize<float>(float t, OutStream& stream) {
    serializeFloat(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeFloat '%f'\n", t);
    }
  }
  template <> void serialize<double>(double t, OutStream& stream) {
    serializeDouble(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeDouble '%f'\n", t);
    }
  }
  template <> void serialize<string>(string t, OutStream& stream) {
    serializeString(t, stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::serializeString '%s'\n", t.c_str());
    }
  }
  
  /**
   * Generic deserialization
   */
  template<class T>
  T deserialize(InStream& stream) {
    string str = "Not able to deserialize type: ";
    throw Error(str.append(typeid(T).name()));
  }
  
  /**
   * Generic deserialization template specializations
   */
  template <> int32_t deserialize<int32_t>(InStream& stream) {
    int32_t result = deserializeInt(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeInt result: '%d'\n",
              result);
    }
    return result;
  }
  template <> int64_t deserialize<int64_t>(InStream& stream) {
    int64_t result = deserializeLong(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeLong result: '%ld'\n",
              (long)result);
    }
    return result;
  }
  template <> float deserialize<float>(InStream& stream) {
    float result = deserializeFloat(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeFloat result: '%f'\n",
              result);
    }
    return result;
  }
  template <> double deserialize<double>(InStream& stream) {
    double result = deserializeDouble(stream);
    if (logging) {
      fprintf(stderr,"HamaPipes::BinaryProtocol::deserializeDouble result: '%f'\n",
              result);
    }
    return result;
  }
  template <> string deserialize<string>(InStream& stream) {
    string result = deserializeString(stream);
    
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
  
  /**
   * Run the assigned task in the framework.
   * The user's main function should set the various functions using the
   * set* functions above and then call this.
   * @return true, if the task succeeded.
   */
  template<class K1, class V1, class K2, class V2, class M>
  bool runTask(const Factory<K1, V1, K2, V2, M>& factory);
  
  // Include implementation in header because of templates
  #include "../../impl/Pipes.cc"
}

#endif
