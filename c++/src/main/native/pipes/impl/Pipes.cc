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

/********************************************/
/*********** BinaryUpwardProtocol ***********/
/********************************************/
class BinaryUpwardProtocol: public UpwardProtocol<BinaryUpwardProtocol> {
private:
  FileOutStream* out_stream_;
public:
  BinaryUpwardProtocol(FILE* out_stream) {
    out_stream_ = new FileOutStream();
    HADOOP_ASSERT(out_stream_->open(out_stream), "problem opening stream");
  }
  
  /* local sendCommand function */
  void sendCommand(int32_t cmd, bool flush) {
    serialize<int32_t>(cmd, *out_stream_);
    if (flush) {
      out_stream_->flush();
    }
    if(logging) {
      fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s\n",
              messageTypeNames[cmd]);
    }
  }
  
  template<class T>
  void sendCommand(int32_t cmd, T value) {
    sendCommand(cmd, false);
    // Write out generic value
    serialize<T>(value, *out_stream_);
    out_stream_->flush();
    if(logging) {
      fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Value: '%s'\n",
              messageTypeNames[cmd], toString<T>(value).c_str());
    }
  }
  
  template<class T>
  void sendCommand(int32_t cmd, const T values[], int size) {
    sendCommand(cmd, false);
    // Write out generic values
    for (int i=0; i<size; i++) {
      serialize<T>(values[i], *out_stream_);
      if(logging) {
        fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param%d: '%s'\n",
                messageTypeNames[cmd], i+1, toString<T>(values[i]).c_str());
      }
    }
    out_stream_->flush();
  }
  
  template<class T1, class T2>
  void sendCommand(int32_t cmd, T1 value1, T2 value2) {
    sendCommand(cmd, false);
    // Write out generic value1
    serialize<T1>(value1, *out_stream_);
    if(logging) {
      fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param1: '%s'\n",
              messageTypeNames[cmd], toString<T1>(value1).c_str());
    }
    // Write out generic value2
    serialize<T2>(value2, *out_stream_);
    if(logging) {
      fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param2: '%s'\n",
              messageTypeNames[cmd], toString<T2>(value2).c_str());
    }
    out_stream_->flush();
  }
  
  template<class T1, class T2>
  void sendCommand(int32_t cmd, T1 value, const T2 values[], int size) {
    sendCommand(cmd, false);
    // Write out generic value
    serialize<T1>(value, *out_stream_);
    if(logging) {
      fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param%d: '%s'\n",
              messageTypeNames[cmd], 0, toString<T1>(value).c_str());
    }
    // Write out generic values
    for (int i=0; i<size; i++) {
      serialize<T2>(values[i], *out_stream_);
      if(logging) {
        fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param%d: '%s'\n",
                messageTypeNames[cmd], i+1, toString<T2>(value).c_str());
      }
    }
    out_stream_->flush();
  }
  
  virtual void sendCommand(int32_t cmd) {
    sendCommand(cmd, true);
  }
  
  /*
   virtual void registerCounter(int id, const string& group,
   const string& name) {
   serialize<int32_t>(REGISTER_COUNTER, *stream);
   serialize<int32_t>(id, *stream);
   serialize<string>(group, *stream);
   serialize<string>(name, *stream);
   }
   
   virtual void incrementCounter(const TaskContext::Counter* counter,
   uint64_t amount) {
   serialize<int32_t>(INCREMENT_COUNTER, *stream);
   serialize<int32_t>(counter->getId(), *stream);
   serialize<int64_t>(amount, *stream);
   }
   */
  
  virtual void incrementCounter(const string& group, const string& name, uint64_t amount) {
    serialize<int32_t>(INCREMENT_COUNTER, *out_stream_);
    serialize<string>(group, *out_stream_);
    serialize<string>(name, *out_stream_);
    serialize<int64_t>(amount, *out_stream_);
    out_stream_->flush();
    if(logging) {
      fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent incrementCounter\n");
    }
  }
  
  ~BinaryUpwardProtocol() {
    delete out_stream_;
  }
};

/********************************************/
/************** BinaryProtocol **************/
/********************************************/
template<class K1, class V1>
class BinaryProtocol: public Protocol< BinaryProtocol<K1,V1> > {
private:
  FileInStream* in_stream_;
  DownwardProtocol<K1,V1>* handler_;
  BinaryUpwardProtocol* uplink_;
  
public:
  BinaryProtocol(FILE* in_stream, DownwardProtocol<K1,V1>* handler, FILE* uplink) {
    in_stream_ = new FileInStream();
    in_stream_->open(in_stream);
    uplink_ = new BinaryUpwardProtocol(uplink);
    handler_ = handler;
  }
  
  UpwardProtocol<BinaryUpwardProtocol>* getUplink() {
    return uplink_;
  }
  
  /**
   * Wait for next event, but don't expect a response for
   * a previously sent command
   */
  void nextEvent() {
    // read command
    int32_t cmd;
    cmd = deserializeInt(*in_stream_);
    
    switch (cmd) {
        
      case START_MESSAGE: {
        int32_t protocol_version;
        protocol_version = deserialize<int32_t>(*in_stream_);
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got START_MESSAGE protocol_version: %d\n",
                  protocol_version);
        }
        handler_->start(protocol_version);
        break;
      }
      // setup BSP Job Configuration
      case SET_BSPJOB_CONF: {
        int32_t entries;
        entries = deserialize<int32_t>(*in_stream_);
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SET_BSPJOB_CONF entries: %d\n",
                  entries);
        }
        vector<string> properties(entries*2);
        for(int i=0; i < entries*2; ++i) {
          string item;
          item = deserialize<string>(*in_stream_);
          properties.push_back(item);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SET_BSPJOB_CONF add NewEntry: %s\n",
                    item.c_str());
          }
        }
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got all Configuration %d entries!\n",
                  entries);
        }
        handler_->setBSPJob(properties);
        break;
      }
      case SET_INPUT_TYPES: {
        string key_type;
        string value_type;
        key_type = deserialize<string>(*in_stream_);
        value_type = deserialize<string>(*in_stream_);
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SET_INPUT_TYPES keyType: %s valueType: %s\n",
                  key_type.c_str(), value_type.c_str());
        }
        handler_->setInputTypes(key_type, value_type);
        break;
      }
      case RUN_SETUP: {
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got RUN_SETUP\n");
        }
        int32_t piped_input;
        int32_t piped_output;
        piped_input = deserialize<int32_t>(*in_stream_);
        piped_output = deserialize<int32_t>(*in_stream_);
        handler_->runSetup(piped_input, piped_output);
        break;
      }
      case RUN_BSP: {
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got RUN_BSP\n");
        }
        int32_t piped_input;
        int32_t piped_output;
        piped_input = deserialize<int32_t>(*in_stream_);
        piped_output = deserialize<int32_t>(*in_stream_);
        handler_->runBsp(piped_input, piped_output);
        break;
      }
      case RUN_CLEANUP: {
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got RUN_CLEANUP\n");
        }
        int32_t piped_input;
        int32_t piped_output;
        piped_input = deserialize<int32_t>(*in_stream_);
        piped_output = deserialize<int32_t>(*in_stream_);
        handler_->runCleanup(piped_input, piped_output);
        break;
      }
      case PARTITION_REQUEST: {
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got PARTITION_REQUEST\n");
        }
        
        K1 partion_key;
        V1 partion_value;
        int32_t num_tasks;
        
        partion_key = deserialize<K1>(*in_stream_);
        partion_value = deserialize<V1>(*in_stream_);
        num_tasks = deserialize<int32_t>(*in_stream_);
        
        handler_->runPartition(partion_key, partion_value, num_tasks);
        break;
      }
      case CLOSE: {
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got CLOSE\n");
        }
        handler_->close();
        break;
      }
      case ABORT: {
        if(logging) {
          fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got ABORT\n");
        }
        handler_->abort();
        break;
      }
      default: {
        HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
        fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - Unknown binary command: %d\n",
                cmd);
      }
    }
  }
  
  /**
   * Check for valid response command
   */
  bool verifyResult(int32_t expected_response_cmd) {
    int32_t response = deserialize<int32_t>(*in_stream_);
    if (response != expected_response_cmd) {
      return false;
    }
    return true;
  }
  
  /**
   * Wait for next event, which should be a response for
   * a previously sent command (expected_response_cmd)
   * and return the generic result
   */
  template<class T>
  T getResult(int32_t expected_response_cmd) {
    
    T result = T();
    
    // read response command
    int32_t cmd = deserialize<int32_t>(*in_stream_);
    
    // check if response is expected
    if (expected_response_cmd == cmd) {
      
      switch (cmd) {
          
        case GET_MSG_COUNT: {
          T msg_count;
          msg_count = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_MSG_COUNT msg_count: '%s'\n",
                    toString<T>(msg_count).c_str());
          }
          return msg_count;
        }
        case GET_MSG: {
          T msg;
          msg = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_MSG msg: '%s'\n",
                    toString<T>(msg).c_str());
          }
          return msg;
        }
        case GET_PEERNAME: {
          T peername;
          peername = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_PEERNAME peername: %s\n",
                    toString<T>(peername).c_str());
          }
          return peername;
        }
        case GET_PEER_INDEX: {
          T peer_index = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_PEER_INDEX peer_index: '%s'\n",
                    toString<T>(peer_index).c_str());
          }
          return peer_index;
        }
        case GET_PEER_COUNT: {
          T peer_count = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_PEER_COUNT peer_count: '%s'\n",
                    toString<T>(peer_count).c_str());
          }
          return peer_count;
        }
        case GET_SUPERSTEP_COUNT: {
          T superstep_count = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_SUPERSTEP_COUNT superstep_count: '%s'\n",
                    toString<T>(superstep_count).c_str());
          }
          return superstep_count;
        }
          
        case SEQFILE_OPEN: {
          T file_id = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_OPEN file_id: '%s'\n",
                    toString<T>(file_id).c_str());
          }
          return file_id;
        }
        case SEQFILE_APPEND: {
          result = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_APPEND result: '%s'\n",
                    toString<T>(result).c_str());
          }
          return result;
        }
        case SEQFILE_CLOSE: {
          result = deserialize<T>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_CLOSE result: '%s'\n",
                    toString<T>(result).c_str());
          }
          return result;
        }
      }
      // Not expected response
    } else {
      
      /*
       case CLOSE: {
       if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got CLOSE\n");
       handler_->close();
       break;
       }
       case ABORT: {
       if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got ABORT\n");
       handler_->abort();
       break;
       }
       */
      HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
      fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent(%d) - Unknown binary command: %d\n",
              expected_response_cmd, cmd);
    }
    return result;
  }
  
  /**
   * Wait for next event, which should be a response for
   * a previously sent command (expected_response_cmd)
   * and return the generic vector result list
   */
  template<class T>
  vector<T> getVectorResult(int32_t expected_response_cmd) {
    
    vector<T> results;
    
    // read response command
    int32_t cmd = deserialize<int32_t>(*in_stream_);
    
    // check if response is expected
    if (expected_response_cmd == cmd) {
      
      switch (cmd) {
        case GET_ALL_PEERNAME: {
          vector<T> peernames;
          T peername;
          int32_t peername_count = deserialize<int32_t>(*in_stream_);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_ALL_PEERNAME peername_count: %d\n",
                    peername_count);
          }
          for (int i=0; i<peername_count; i++)  {
            peername = deserialize<T>(*in_stream_);
            peernames.push_back(peername);
            if(logging) {
              fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_ALL_PEERNAME peername: '%s'\n",
                      toString<T>(peername).c_str());
            }
          }
          return peernames;
        }
      }
    } else {
      HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
      fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent(%d) - Unknown binary command: %d\n",
              expected_response_cmd, cmd);
    }
    return results;
  }
  
  /**
   * Wait for next event, which should be a response for
   * a previously sent command (expected_response_cmd)
   * and return the generic KeyValuePair or an empty one
   * if no data is available
   */
  template <class K, class V>
  KeyValuePair<K,V> getKeyValueResult(int32_t expected_response_cmd) {
    
    KeyValuePair<K,V> key_value_pair;
    
    // read response command
    int32_t cmd = deserialize<int32_t>(*in_stream_);
    
    // check if response is expected or END_OF_DATA
    if ((expected_response_cmd == cmd) || (cmd == END_OF_DATA) ) {
      
      switch (cmd) {
          
        case READ_KEYVALUE: {
          K key = deserialize<K>(*in_stream_);
          V value = deserialize<V>(*in_stream_);
          
          if(logging) {
            string k = toString<K>(key);
            string v = toString<V>(value);
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got READ_KEYVALUE key: '%s' value: '%s'\n",
                    ((k.length()<10)?k.c_str():k.substr(0,9).append("...").c_str()),
                    ((v.length()<10)?v.c_str():v.substr(0,9).append("...").c_str()) );
          }
          
          key_value_pair = pair<K,V>(key, value);
          return key_value_pair;
        }
        case SEQFILE_READNEXT: {
          K key = deserialize<K>(*in_stream_);
          V value = deserialize<V>(*in_stream_);
          
          if(logging) {
            string k = toString<K>(key);
            string v = toString<V>(value);
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_READNEXT key: '%s' value: '%s'\n",
                    ((k.length()<10)?k.c_str():k.substr(0,9).append("...").c_str()),
                    ((v.length()<10)?v.c_str():v.substr(0,9).append("...").c_str()) );
          }
          
          key_value_pair = pair<K,V>(key, value);
          return key_value_pair;
        }
        case END_OF_DATA: {
          key_value_pair = KeyValuePair<K,V>(true);
          if(logging) {
            fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got END_OF_DATA\n");
          }
        }
      }
    } else {
      key_value_pair = KeyValuePair<K,V>(true);
      fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent(expected_cmd = %d) - Unknown binary command: %d\n",
              expected_response_cmd, cmd);
      fprintf(stderr,"ERORR: Please verfiy serialization! The key or value type could possibly not be deserialized!\n");
      HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
    }
    return key_value_pair;
  }
  
  virtual ~BinaryProtocol() {
    delete in_stream_;
    delete uplink_;
    delete handler_;
  }
};

/********************************************/
/************** BSPContextImpl **************/
/********************************************/
template<class K1, class V1, class K2, class V2, class M>
class BSPContextImpl: public BSPContext<K1, V1, K2, V2, M>, public DownwardProtocol<K1, V1> {
private:
  const Factory<K1, V1, K2, V2, M>* factory_;
  BSPJob* job_;
  BSP<K1, V1, K2, V2, M>* bsp_;
  Partitioner<K1, V1, K2, V2, M>* partitioner_;
  RecordReader<K1, V1>* reader_;
  RecordWriter<K2, V2>* writer_;
  Protocol< BinaryProtocol<K1,V1> >* protocol_;
  UpwardProtocol<BinaryUpwardProtocol>* uplink_;
  
  bool done_;
  bool has_task_;
  pthread_mutex_t mutex_done_;
  std::vector<int> registered_counter_ids_;
  
  pair<string, string> inputClass_;
  //string* inputSplit_;
  
public:
  
  BSPContextImpl(const Factory<K1, V1, K2, V2, M>& factory) {
    
    factory_ = &factory;
    job_ = NULL;
    bsp_ = NULL;
    partitioner_ = NULL;
    reader_ = NULL;
    writer_ = NULL;
    protocol_ = NULL;
    uplink_ = NULL;
    
    done_ = false;
    has_task_ = false;
    pthread_mutex_init(&mutex_done_, NULL);
    
    //inputSplit_ = NULL;
  }
  
  
  /********************************************/
  /*********** DownwardProtocol IMPL **********/
  /********************************************/
  virtual void start(int protocol_version) {
    if (protocol_version != 0) {
      throw Error("Protocol version " + toString(protocol_version) +
                  " not supported");
    }
    partitioner_ = factory_->createPartitioner(*this);
  }
  
  virtual void setBSPJob(vector<string> values) {
    int len = values.size();
    BSPJobImpl* result = new BSPJobImpl();
    HADOOP_ASSERT(len % 2 == 0, "Odd length of job conf values");
    for(int i=0; i < len; i += 2) {
      result->set(values[i], values[i+1]);
    }
    job_ = result;
  }
  
  virtual void setInputTypes(string key_type, string value_type) {
    inputClass_ = pair<string,string>(key_type, value_type);
  }
  
  /* local method */
  void setupReaderWriter(bool piped_input, bool piped_output) {
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::setupReaderWriter - pipedInput: %s pipedOutput: %s\n",
              (piped_input)?"true":"false", (piped_output)?"true":"false");
    }
    
    if (piped_input && reader_==NULL) {
      reader_ = factory_->createRecordReader(*this);
      HADOOP_ASSERT((reader_ == NULL) == piped_input,
                    piped_input ? "RecordReader defined when not needed.":
                    "RecordReader not defined");
      
      //if (reader != NULL) {
      //  value = new string();
      //}
    }
    
    if (piped_output && writer_==NULL) {
      writer_ = factory_->createRecordWriter(*this);
      HADOOP_ASSERT((writer_ == NULL) == piped_output,
                    piped_output ? "RecordWriter defined when not needed.":
                    "RecordWriter not defined");
    }
  }
  
  virtual void runSetup(bool piped_input, bool piped_output) {
    setupReaderWriter(piped_input, piped_output);
    
    if (bsp_ == NULL) {
      bsp_ = factory_->createBSP(*this);
    }
    
    if (bsp_ != NULL) {
      has_task_ = true;
      bsp_->setup(*this);
      has_task_ = false;
      uplink_->sendCommand(TASK_DONE);
    }
  }
  
  virtual void runBsp(bool piped_input, bool piped_output) {
    setupReaderWriter(piped_input, piped_output);
    
    if (bsp_ == NULL) {
      bsp_ = factory_->createBSP(*this);
    }
    
    if (bsp_ != NULL) {
      has_task_ = true;
      bsp_->bsp(*this);
      has_task_ = false;
      uplink_->sendCommand(TASK_DONE);
    }
  }
  
  virtual void runCleanup(bool piped_input, bool piped_output) {
    setupReaderWriter(piped_input, piped_output);
    
    if (bsp_ != NULL) {
      has_task_ = true;
      bsp_->cleanup(*this);
      has_task_ = false;
      uplink_->sendCommand(TASK_DONE);
    }
  }
  
  /********************************************/
  /*******       Partitioner            *******/
  /********************************************/
  virtual void runPartition(const K1& key, const V1& value, int32_t num_tasks) {
    if (partitioner_ != NULL) {
      int part = partitioner_->partition(key, value, num_tasks);
      uplink_->sendCommand<int32_t>(PARTITION_RESPONSE, part);
    } else {
      if(logging) {
        fprintf(stderr,"HamaPipes::BSPContextImpl::runPartition Partitioner is NULL!\n");
      }
    }
  }
  
  virtual void close() {
    pthread_mutex_lock(&mutex_done_);
    done_ = true;
    has_task_ = false;
    pthread_mutex_unlock(&mutex_done_);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::close - done: %s hasTask: %s\n",
              (done_)?"true":"false",(has_task_)?"true":"false");
    }
  }
  
  virtual void abort() {
    throw Error("Aborted by driver");
  }
  
  /********************************************/
  /************** TaskContext IMPL ************/
  /********************************************/
  
  /**
   * Get the BSPJob for the current task.
   */
  virtual const BSPJob* getBSPJob() {
    return job_;
  }
  
  /**
   * Get the current key.
   * @return the current key or NULL if called before the first map or reduce
   */
  //virtual const string& getInputKey() {
  //  return key;
  //}
  
  /**
   * Get the current value.
   * @return the current value or NULL if called before the first map or
   *    reduce
   */
  //virtual const string& getInputValue() {
  //  return *value;
  //}
  
  /**
   * Register a counter with the given group and name.
   */
  virtual long getCounter(const string& group, const string& name) {
   // TODO
   // int id = registeredCounterIds.size();
   // registeredCounterIds.push_back(id);
   // uplink->registerCounter(id, group, name);
   // return new Counter(id);
   return 0;
  }
  
  /**
   * Increments the counter identified by the group and counter name by the
   * specified amount.
   */
  virtual void incrementCounter(const string& group, const string& name, uint64_t amount)  {
    uplink_->incrementCounter(group, name, amount);
    
    // Verify response command
    bool response = protocol_->verifyResult(INCREMENT_COUNTER);
    if (response == false) {
      throw Error("incrementCounter received wrong response!");
    }
  }
  
  /********************************************/
  /************** BSPContext IMPL *************/
  /********************************************/
  
  /**
   * Access the InputSplit of the bsp.
   */
  //virtual const string& getInputSplit() {
  //  return *inputSplit_;
  //}
  
  /**
   * Get the name of the key class of the input to this task.
   */
  virtual string getInputKeyClass() {
    return inputClass_.first;
  }
  
  /**
   * Get the name of the value class of the input to this task.
   */
  virtual string getInputValueClass() {
    return inputClass_.second;
  }
  
  /**
   * Send a data with a tag to another BSPSlave corresponding to hostname.
   * Messages sent by this method are not guaranteed to be received in a sent
   * order.
   */
  virtual void sendMessage(const string& peer_name, const M& msg) {
    uplink_->sendCommand<string,M>(SEND_MSG, peer_name, msg);
    
    // Verify response command
    bool response = protocol_->verifyResult(SEND_MSG);
    if (response == false) {
      throw Error("sendMessage received wrong response!");
    }
  }
  
  /**
   * @return A message from the peer's received messages queue (a FIFO).
   */
  virtual M getCurrentMessage() {
    uplink_->sendCommand(GET_MSG);
    
    M message = protocol_->template getResult<M>(GET_MSG);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getMessage - result: %s\n",
              toString<M>(message).c_str());
    }
    return message;
  }
  
  /**
   * @return The number of messages in the peer's received messages queue.
   */
  virtual int getNumCurrentMessages() {
    uplink_->sendCommand(GET_MSG_COUNT);
    
    int result = protocol_->template getResult<int32_t>(GET_MSG_COUNT);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getNumCurrentMessages - result: %d\n",
              result);
    }
    return result;
  }
  
  /**
   * Barrier Synchronization.
   *
   * Sends all the messages in the outgoing message queues to the corresponding
   * remote peers.
   */
  virtual void sync() {
    uplink_->sendCommand(SYNC);
    
    // Verify response command
    bool response = protocol_->verifyResult(SYNC);
    if (response == false) {
      throw Error("sync received wrong response!");
    }
  }
  
  /**
   * @return the name of this peer in the format "hostname:port".
   */
  virtual string getPeerName() {
    // submit id=-1 to receive own peername
    uplink_->sendCommand<int32_t>(GET_PEERNAME, -1);
    
    string result = protocol_->template getResult<string>(GET_PEERNAME);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getPeerName - result: %s\n",
              result.c_str());
    }
    return result;
  }
  
  /**
   * @return the name of n-th peer from sorted array by name.
   */
  virtual string getPeerName(int index) {
    uplink_->sendCommand<int32_t>(GET_PEERNAME, index);
    
    string result = protocol_->template getResult<string>(GET_PEERNAME);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getPeerName - result: %s\n",
              result.c_str());
    }
    
    return result;
  }
  
  /**
   * @return the names of all the peers executing tasks from the same job
   *         (including this peer).
   */
  virtual vector<string> getAllPeerNames() {
    uplink_->sendCommand(GET_ALL_PEERNAME);
    
    vector<string> results = protocol_->template getVectorResult<string>(GET_ALL_PEERNAME);
    
    return results;
  }
  
  /**
   * @return the index of this peer from sorted array by name.
   */
  virtual int getPeerIndex() {
    uplink_->sendCommand(GET_PEER_INDEX);
    
    int result = protocol_->template getResult<int32_t>(GET_PEER_INDEX);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getPeerIndex - result: %d\n",
              result);
    }
    return result;
  }
  
  /**
   * @return the number of peers
   */
  virtual int getNumPeers() {
    uplink_->sendCommand(GET_PEER_COUNT);
    
    int result = protocol_->template getResult<int32_t>(GET_PEER_COUNT);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getNumPeers - result: %d\n",
              result);
    }
    return result;
  }
  
  /**
   * @return the count of current super-step
   */
  virtual long getSuperstepCount() {
    uplink_->sendCommand(GET_SUPERSTEP_COUNT);
    
    long result = protocol_->template getResult<int64_t>(GET_SUPERSTEP_COUNT);
    
    if(logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::getSuperstepCount - result: %ld\n",
              result);
    }
    return result;
  }
  
  /**
   * Clears all queues entries.
   */
  virtual void clear() {
    uplink_->sendCommand(CLEAR);
    
    // Verify response command
    bool response = protocol_->verifyResult(CLEAR);
    if (response == false) {
      throw Error("clear received wrong response!");
    }
  }
  
  /**
   * Writes a key/value pair to the output collector
   */
  virtual void write(const K2& key, const V2& value) {
    if (writer_ != NULL) {
      writer_->emit(key, value); // TODO writer not implemented
    } else {
      uplink_->sendCommand<K2,V2>(WRITE_KEYVALUE, key, value);
    }

    // Verify response command
    bool response = protocol_->verifyResult(WRITE_KEYVALUE);
    if (response == false) {
      throw Error("write received wrong response!");
    }
  }
  
  /**
   * Deserializes the next input key value into the given objects;
   */
  virtual bool readNext(K1& key, V1& value) {
    
    uplink_->sendCommand(READ_KEYVALUE);
    
    KeyValuePair<K1,V1> key_value_pair;
    key_value_pair = protocol_->template getKeyValueResult<K1,V1>(READ_KEYVALUE);
    
    if (!key_value_pair.is_empty) {
      key = key_value_pair.first;
      value = key_value_pair.second;
    }
    
    if (logging && key_value_pair.is_empty) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::readNext - END_OF_DATA\n");
    }
    
    return (!key_value_pair.is_empty);
  }
  
  /**
   * Closes the input and opens it right away, so that the file pointer is at
   * the beginning again.
   */
  virtual void reopenInput() {
    uplink_->sendCommand(REOPEN_INPUT);
    
    // Verify response command
    bool response = protocol_->verifyResult(REOPEN_INPUT);
    if (response == false) {
      throw Error("reopenInput received wrong response!");
    }
  }
  
  
  /********************************************/
  /*******  SequenceFileConnector IMPL  *******/
  /********************************************/
  
  /**
   * Open SequenceFile with opion "r" or "w"
   * @return the corresponding fileID
   */
  virtual int32_t sequenceFileOpen(const string& path, const string& option,
                                   const string& key_type, const string& value_type) {
    if (logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileOpen - Path: %s\n",
              path.c_str());
    }
    
    if ( (option.compare("r")==0) || (option.compare("w")==0))  {
      
      string values[] = {path, option, key_type, value_type};
      uplink_->sendCommand<string>(SEQFILE_OPEN, values, 4);
      
      int result = protocol_->template getResult<int32_t>(SEQFILE_OPEN);
      
      if(logging) {
        fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileOpen - result: %d\n",
                result);
      }
      return result;
    } else {
      //Error wrong option
      fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileOpen wrong option: %s!\n",
              option.c_str());
      return -1;
    }
  }
  
  /**
   * Close SequenceFile
   */
  virtual bool sequenceFileClose(int32_t file_id) {
    uplink_->sendCommand<int32_t>(SEQFILE_CLOSE, file_id);
    
    int result = protocol_->template getResult<int32_t>(SEQFILE_CLOSE);
    
    if (logging && result==0) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileClose - Nothing was closed!\n");
    } else if (logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileClose - File was successfully closed!\n");
    }
    
    return (result==1);
  }
  
  /**
   * Read next key/value pair from the SequenceFile with fileID
   * Using Curiously recurring template pattern(CTRP)
   */
  template<class K, class V>
  bool sequenceFileReadNext(int32_t file_id, K& key, V& value) {
    
    // send request
    uplink_->sendCommand<int32_t>(SEQFILE_READNEXT, file_id);
    
    // get response
    KeyValuePair<K,V> key_value_pair;
    key_value_pair = protocol_->template getKeyValueResult<K,V>(SEQFILE_READNEXT);
    
    if (!key_value_pair.is_empty) {
      key = key_value_pair.first;
      value = key_value_pair.second;
    }
    
    if (logging && key_value_pair.is_empty) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::readNext - END_OF_DATA\n");
    }
    
    return (!key_value_pair.is_empty);
  }
  
  /**
   * Append the next key/value pair to the SequenceFile with fileID
   * Using Curiously recurring template pattern(CTRP)
   */
  template<class K, class V>
  bool sequenceFileAppend(int32_t file_id, const K& key, const V& value) {
    string values[] = {key, value};
    uplink_->sendCommand<int32_t,string>(SEQFILE_APPEND, file_id, values, 2);
    
    int result = protocol_->template getResult<int32_t>(SEQFILE_APPEND);
    
    if (logging && result==0) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileAppend - Nothing appended!\n");
    } else if (logging) {
      fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileAppend - Successfully appended!\n");
    }
    
    return (result==1);
  }
  
  /********************************************/
  /*************** Other STUFF  ***************/
  /********************************************/
  
  void setProtocol(Protocol< BinaryProtocol<K1,V1> >* protocol, UpwardProtocol<BinaryUpwardProtocol>* uplink) {
    protocol_ = protocol;
    uplink_ = uplink;
  }
  
  bool isDone() {
    pthread_mutex_lock(&mutex_done_);
    bool done_copy = done_;
    pthread_mutex_unlock(&mutex_done_);
    return done_copy;
  }
  
  /**
   * Advance to the next value.
   */
  /*
   bool nextValue() {
   if (isNewKey || done) {
   return false;
   }
   isNewValue = false;
   //progress();
   protocol->nextEvent();
   return isNewValue;
   }
   */
  void waitForTask() {
    while (!done_ && !has_task_) {
      if(logging) {
        fprintf(stderr,"HamaPipes::BSPContextImpl::waitForTask - done: %s hasTask: %s\n",
                (done_)?"true":"false", (has_task_)?"true":"false");
      }
      protocol_->nextEvent();
    }
  }
  /*
   bool nextKey() {
   if (reader == NULL) {
   while (!isNewKey) {
   nextValue();
   if (done) {
   return false;
   }
   }
   key = *newKey;
   } else {
   if (!reader->next(key, const_cast<string&>(*value))) {
   pthread_mutex_lock(&mutexDone);
   done = true;
   pthread_mutex_unlock(&mutexDone);
   return false;
   }
   //progressFloat = reader->getProgress();
   }
   isNewKey = false;
   
   if (bsp != NULL) {
   bsp->bsp(*this);
   }
   return true;
   }
   */
  void closeAll() {
    if (reader_) {
      reader_->close();
    }
    
    if (bsp_) {
      bsp_->close();
    }
    
    if (writer_) {
      writer_->close();
    }
  }
  
  virtual ~BSPContextImpl() {
    delete factory_;
    delete job_;
    delete bsp_;
    delete partitioner_;
    delete reader_;
    delete writer_;
    delete protocol_;
    delete uplink_;
    //delete inputSplit_;
    pthread_mutex_destroy(&mutex_done_);
  }
};

/**
 * Ping the parent every 5 seconds to know if it is alive
 */
template<class K1, class V1, class K2, class V2, class M>
void* ping(void* ptr) {
  BSPContextImpl<K1, V1, K2, V2, M>* context = (BSPContextImpl<K1, V1, K2, V2, M>*) ptr;
  char* portStr = getenv("hama.pipes.command.port");
  int MAX_RETRIES = 3;
  int remaining_retries = MAX_RETRIES;
  while (!context->isDone()) {
    try{
      sleep(5);
      int sock = -1;
      if (portStr) {
        sock = socket(PF_INET, SOCK_STREAM, 0);
        HADOOP_ASSERT(sock != - 1,
                      string("problem creating socket: ") + strerror(errno));
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(toInt(portStr));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if(logging) {
          fprintf(stderr,"HamaPipes::ping - connected to GroomServer Port: %s\n",
                  portStr);
        }
        HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                      string("problem connecting command socket: ") +
                      strerror(errno));
        
      }
      if (sock != -1) {
        int result = shutdown(sock, SHUT_RDWR);
        HADOOP_ASSERT(result == 0, "problem shutting socket");
        result = close(sock);
        HADOOP_ASSERT(result == 0, "problem closing socket");
      }
      remaining_retries = MAX_RETRIES;
    } catch (Error& err) {
      if (!context->isDone()) {
        fprintf(stderr, "Hama Pipes Exception: in ping %s\n",
                err.getMessage().c_str());
        remaining_retries -= 1;
        if (remaining_retries == 0) {
          exit(1);
        }
      } else {
        return NULL;
      }
    }
  }
  return NULL;
}

/**
 * Run the assigned task in the framework.
 * The user's main function should set the various functions using the
 * set* functions above and then call this.
 * @return true, if the task succeeded.
 */
template<class K1, class V1, class K2, class V2, class M>
bool runTask(const Factory<K1, V1, K2, V2, M>& factory) {
  try {
    HADOOP_ASSERT(getenv("hama.pipes.logging")!=NULL, "No environment found!");
    
    logging = (toInt(getenv("hama.pipes.logging"))==0)?false:true;
    if (logging) {
      fprintf(stderr,"HamaPipes::runTask - logging is: %s\n",
              ((logging)?"true":"false"));
    }
    
    BSPContextImpl<K1, V1, K2, V2, M>* context = new BSPContextImpl<K1, V1, K2, V2, M>(factory);
    Protocol< BinaryProtocol<K1,V1> >* protocol;
    
    char* port_str = getenv("hama.pipes.command.port");
    int sock = -1;
    FILE* in_stream = NULL;
    FILE* out_stream = NULL;
    char *bufin = NULL;
    char *bufout = NULL;
    if (port_str) {
      sock = socket(PF_INET, SOCK_STREAM, 0);
      HADOOP_ASSERT(sock != - 1,
                    string("problem creating socket: ") + strerror(errno));
      sockaddr_in addr;
      addr.sin_family = AF_INET;
      addr.sin_port = htons(toInt(port_str));
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                    string("problem connecting command socket: ") +
                    strerror(errno));
      
      in_stream = fdopen(sock, "r");
      out_stream = fdopen(sock, "w");
      
      // increase buffer size
      int bufsize = 128*1024;
      int setbuf;
      bufin = new char[bufsize];
      bufout = new char[bufsize];
      setbuf = setvbuf(in_stream, bufin, _IOFBF, bufsize);
      HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for in_stream: ")
                    + strerror(errno));
      setbuf = setvbuf(out_stream, bufout, _IOFBF, bufsize);
      HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for out_stream: ")
                    + strerror(errno));
      
      protocol = new BinaryProtocol<K1,V1>(in_stream, context, out_stream);
      if(logging) {
        fprintf(stderr,"HamaPipes::runTask - connected to GroomServer Port: %s\n",
                port_str);
      }
      
    } else if (getenv("hama.pipes.command.file")) {
      char* filename = getenv("hama.pipes.command.file");
      string out_filename = filename;
      out_filename += ".out";
      in_stream = fopen(filename, "r");
      out_stream = fopen(out_filename.c_str(), "w");
      protocol = new BinaryProtocol<K1,V1>(in_stream, context, out_stream);
    } else {
      //protocol = new TextProtocol(stdin, context, stdout);
      fprintf(stderr,"HamaPipes::runTask - Protocol couldn't be initialized!\n");
      return -1;
    }
    
    context->setProtocol(protocol, protocol->getUplink());
    
    //pthread_t pingThread;
    //pthread_create(&pingThread, NULL, ping, (void*)(context));
    
    context->waitForTask();
    
    context->closeAll();
    protocol->getUplink()->sendCommand(DONE);
    
    //pthread_join(pingThread,NULL);
    
    if (in_stream != NULL) {
      fflush(in_stream);
    }
    if (out_stream != NULL) {
      fflush(out_stream);
    }
    
    fflush(stdout);
    
    if (sock != -1) {
      int result = shutdown(sock, SHUT_RDWR);
      HADOOP_ASSERT(result == 0, "problem shutting socket");
      result = close(sock);
      HADOOP_ASSERT(result == 0, "problem closing socket");
    }
    
    // Cleanup
    delete context;
    delete protocol;
    
    delete bufin;
    delete bufout;
    
    delete in_stream;
    delete out_stream;
    
    return true;
    
  } catch (Error& err) {
    fprintf(stderr, "Hama Pipes Exception: %s\n",
            err.getMessage().c_str());
    return false;
  }
}
