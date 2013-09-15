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

#include "hama/Pipes.hh"
#include "hadoop/SerialUtils.hh"
#include "hadoop/StringUtils.hh"

#include <map>
#include <vector>

#include <unistd.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <pthread.h>
#include <iostream>
#include <fstream>

#include <openssl/hmac.h>
#include <openssl/buffer.h>

#define stringify( name ) # name

using std::map;
using std::string;
using std::vector;
using std::cout;
using std::endl;

using namespace HadoopUtils;

namespace HamaPipes {

  bool logging;
  
  /********************************************/
  /****************** BSPJob ******************/  
  /********************************************/
  class BSPJobImpl: public BSPJob {
  private:
    map<string, string> values;
  public:
    void set(const string& key, const string& value) {
      values[key] = value;
    }

    virtual bool hasKey(const string& key) const {
      return values.find(key) != values.end();
    }

    virtual const string& get(const string& key) const {
      map<string,string>::const_iterator itr = values.find(key);
      if (itr == values.end()) {
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

    virtual bool getBoolean(const string&key) const {
      const string& val = get(key);
      return toBool(val);
    }
  };
    
  /********************************************/
  /************* DownwardProtocol *************/  
  /********************************************/
  class DownwardProtocol {
  public:
    virtual void start(int protocol) = 0;
    virtual void setBSPJob(vector<string> values) = 0;
    virtual void setInputTypes(string keyType, string valueType) = 0;
    virtual void setKeyValue(const string& _key, const string& _value) = 0;
      
    virtual void runBsp(bool pipedInput, bool pipedOutput) = 0;
    virtual void runCleanup(bool pipedInput, bool pipedOutput) = 0;
    virtual void runSetup(bool pipedInput, bool pipedOutput) = 0;
    virtual void runPartition(const string& key, const string& value, int32_t numTasks) = 0;  
      
    virtual void setNewResult(int32_t value) = 0;
    virtual void setNewResult(int64_t value) = 0;  
    virtual void setNewResult(const string&  value) = 0;
    virtual void setNewResult(vector<string> value) = 0;
      
    //virtual void reduceKey(const string& key) = 0;
    //virtual void reduceValue(const string& value) = 0;
    virtual void close() = 0;
    virtual void abort() = 0;
    virtual ~DownwardProtocol() {}
  };

  /********************************************/
  /************** UpwardProtocol **************/  
  /********************************************/
  class UpwardProtocol {
  public:
    virtual void sendCMD(int32_t cmd) = 0;
    virtual void sendCMD(int32_t cmd, int32_t value) = 0;
    virtual void sendCMD(int32_t cmd, int32_t value, const string values[], int size) = 0;
    virtual void sendCMD(int32_t cmd, const string& value) = 0;
    virtual void sendCMD(int32_t cmd, const string values[], int size) = 0;
      
    //virtual void registerCounter(int id, const string& group, const string& name) = 0;
    //virtual void incrementCounter(const TaskContext::Counter* counter, uint64_t amount) = 0;
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) = 0;
    virtual ~UpwardProtocol() {}
  };
    
  /********************************************/
  /***************** Protocol *****************/  
  /********************************************/
  class Protocol {
  public:
    virtual void nextEvent() = 0;
    virtual UpwardProtocol* getUplink() = 0;
    virtual ~Protocol(){}
  };
    
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
      PARTITION_REQUEST, PARTITION_RESPONSE
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
      stringify( PARTITION_REQUEST ), stringify( PARTITION_RESPONSE )
    };

  /********************************************/
  /*********** BinaryUpwardProtocol ***********/  
  /********************************************/
  class BinaryUpwardProtocol: public UpwardProtocol {
  private:
    FileOutStream* stream;
  public:
    BinaryUpwardProtocol(FILE* _stream) {
      stream = new FileOutStream();
      HADOOP_ASSERT(stream->open(_stream), "problem opening stream");
        
    }

    virtual void sendCMD(int32_t cmd) {
      serializeInt(cmd, *stream);
      stream->flush();
      if(logging)fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s\n",
          messageTypeNames[cmd]);
    }
      
    virtual void sendCMD(int32_t cmd, int32_t value) {
      serializeInt(cmd, *stream);
      serializeInt(value, *stream);
      stream->flush();
      if(logging)fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Value: %d\n",messageTypeNames[cmd],value);
    }
      
    virtual void sendCMD(int32_t cmd, const string& value) {
      serializeInt(cmd, *stream);
      serializeString(value, *stream);
      stream->flush();
      if(logging)fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Value: %s\n",messageTypeNames[cmd],value.c_str());
    }
      
    virtual void sendCMD(int32_t cmd, const string values[], int size) {
      serializeInt(cmd, *stream);      
      for (int i=0; i<size; i++) { 
        serializeString(values[i], *stream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param%d: %s\n",messageTypeNames[cmd],i+1,values[i].c_str());
      }
      stream->flush();
    }
      
    virtual void sendCMD(int32_t cmd, int32_t value, const string values[], int size) {
      serializeInt(cmd, *stream);
      serializeInt(value, *stream);
      for (int i=0; i<size; i++) { 
        serializeString(values[i], *stream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent CMD: %s with Param%d: %s\n",messageTypeNames[cmd],i+1,values[i].c_str());
      } 
      stream->flush();
    }

    /*
    virtual void registerCounter(int id, const string& group, 
                                 const string& name) {
      serializeInt(REGISTER_COUNTER, *stream);
      serializeInt(id, *stream);
      serializeString(group, *stream);
      serializeString(name, *stream);
    }
    
    virtual void incrementCounter(const TaskContext::Counter* counter, 
                                  uint64_t amount) {
      serializeInt(INCREMENT_COUNTER, *stream);
      serializeInt(counter->getId(), *stream);
      serializeLong(amount, *stream);
    }
    */
    
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) {
      serializeInt(INCREMENT_COUNTER, *stream);
      serializeString(group, *stream);
      serializeString(name, *stream);
      serializeLong(amount, *stream);
      stream->flush();
      if(logging)fprintf(stderr,"HamaPipes::BinaryUpwardProtocol sent incrementCounter\n");
    }
      
    ~BinaryUpwardProtocol() {
      delete stream;
    }
  };

  /********************************************/
  /************** BinaryProtocol **************/  
  /********************************************/
  class BinaryProtocol: public Protocol {
  private:
    FileInStream* downStream;
    DownwardProtocol* handler;
    BinaryUpwardProtocol * uplink;
      
    string key;
    string value;
   
  public:
    BinaryProtocol(FILE* down, DownwardProtocol* _handler, FILE* up) {
      downStream = new FileInStream();
      downStream->open(down);
      uplink = new BinaryUpwardProtocol(up);
      handler = _handler;
      
      //authDone = false;
      //getPassword(password);
    }

    UpwardProtocol* getUplink() {
      return uplink;
    }

      
    virtual void nextEvent() {
      int32_t cmd;
      cmd = deserializeInt(*downStream);
        
     switch (cmd) {
            
      case START_MESSAGE: {
        int32_t prot;
        prot = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got START_MESSAGE prot: %d\n", prot); 
        handler->start(prot);
        break;
      }
      /* SET BSP Job Configuration / Environment */
      case SET_BSPJOB_CONF: {
        int32_t entries;
        entries = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SET_BSPJOB_CONF entries: %d\n", entries); 
        vector<string> result(entries*2);
        for(int i=0; i < entries*2; ++i) {
          string item;
          deserializeString(item, *downStream);
          result.push_back(item);
          if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SET_BSPJOB_CONF add NewEntry: %s\n", item.c_str()); 
        }
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got all Configuration %d entries!\n", entries);
        handler->setBSPJob(result);
        break;
      }
      case SET_INPUT_TYPES: {
        string keyType;
        string valueType;
        deserializeString(keyType, *downStream);
        deserializeString(valueType, *downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SET_INPUT_TYPES keyType: %s valueType: %s\n",
                keyType.c_str(),valueType.c_str()); 
        handler->setInputTypes(keyType, valueType);
        break;
      }
      case READ_KEYVALUE: {
        deserializeString(key, *downStream);
        deserializeString(value, *downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got READ_KEYVALUE key: %s value: %s\n",
                key.c_str(),
                ((value.length()<10)?value.c_str():value.substr(0,9).c_str()) ); 
        handler->setKeyValue(key, value);
        break;
      }
      case RUN_SETUP: {
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got RUN_SETUP\n"); 
        int32_t pipedInput;
        int32_t pipedOutput;
        pipedInput = deserializeInt(*downStream);
        pipedOutput = deserializeInt(*downStream);
        handler->runSetup(pipedInput, pipedOutput);
        break;
      }
      case RUN_BSP: {
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got RUN_BSP\n"); 
        int32_t pipedInput;
        int32_t pipedOutput;
        pipedInput = deserializeInt(*downStream);
        pipedOutput = deserializeInt(*downStream);
        handler->runBsp(pipedInput, pipedOutput);
        break;
      }
      case RUN_CLEANUP: {
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got RUN_CLEANUP\n"); 
        int32_t pipedInput;
        int32_t pipedOutput;
        pipedInput = deserializeInt(*downStream);
        pipedOutput = deserializeInt(*downStream);
        handler->runCleanup(pipedInput, pipedOutput);
        break;
      }
      
      case PARTITION_REQUEST: {
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got PARTITION_REQUEST\n"); 
        string partionKey;
        string partionValue;
        int32_t numTasks;
        deserializeString(partionKey, *downStream);
        deserializeString(partionValue, *downStream);
        numTasks = deserializeInt(*downStream);
        handler->runPartition(partionKey, partionValue, numTasks);
        break;
      }

        
      case GET_MSG_COUNT: {
        int32_t msgCount = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_MSG_COUNT msgCount: %d\n",msgCount); 
        handler->setNewResult(msgCount);
        break;
      }
      case GET_MSG: {
        string msg;
        deserializeString(msg,*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_MSG msg: %s\n",msg.c_str());
        handler->setNewResult(msg);
        break;
      }
      case GET_PEERNAME: {
        string peername;
        deserializeString(peername,*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_PEERNAME peername: %s\n",peername.c_str());
        handler->setNewResult(peername);
        break;
      }
      case GET_ALL_PEERNAME: {
        vector<string> peernames;
        int32_t peernameCount = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_ALL_PEERNAME peernameCount: %d\n",peernameCount);
        string peername;
        for (int i=0; i<peernameCount; i++)  {
          deserializeString(peername,*downStream);
          peernames.push_back(peername);
          if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_ALL_PEERNAME peername: %s\n",peername.c_str());
        }
        handler->setNewResult(peernames);
        break;
      }
      case GET_PEER_INDEX: {
        int32_t peerIndex = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_PEER_INDEX peerIndex: %d\n",peerIndex); 
        handler->setNewResult(peerIndex);
        break;
      }
      case GET_PEER_COUNT: {
        int32_t peerCount = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_PEER_COUNT peerCount: %d\n",peerCount); 
        handler->setNewResult(peerCount);
        break;
      }
      case GET_SUPERSTEP_COUNT: {
        int64_t superstepCount = deserializeLong(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got GET_SUPERSTEP_COUNT superstepCount: %ld\n",(long)superstepCount); 
        handler->setNewResult(superstepCount);
        break;
      }
             
             
      case SEQFILE_OPEN: {
        int32_t fileID = deserializeInt(*downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_OPEN fileID: %d\n",fileID); 
        handler->setNewResult(fileID);
        break;
      }    
      case SEQFILE_READNEXT: {
        deserializeString(key, *downStream);
        deserializeString(value, *downStream);
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_READNEXT key: %s value: %s\n", 
                key.c_str(),
                ((value.length()<10)?value.c_str():value.substr(0,9).c_str()) ); 
        handler->setKeyValue(key, value);
        break;
      }
      case SEQFILE_APPEND: {
          int32_t result = deserializeInt(*downStream);
          if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_APPEND result: %d\n",result);
          handler->setNewResult(result);
          break;
      }   
      case SEQFILE_CLOSE: {
          int32_t result = deserializeInt(*downStream);
          if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got SEQFILE_CLOSE result: %d\n",result);
          handler->setNewResult(result);
          break;
      }
             
        
      case CLOSE: {
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got CLOSE\n"); 
        handler->close();
        break;
      }
      case ABORT: {
        if(logging)fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - got ABORT\n"); 
        handler->abort();
        break;
      }        
      default:
        HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
        fprintf(stderr,"HamaPipes::BinaryProtocol::nextEvent - Unknown binary command: %d\n",cmd); 
      }
     }
      
    virtual ~BinaryProtocol() {
      delete downStream;
      delete uplink;
    }
  };

  /********************************************/
  /************** BSPContextImpl **************/  
  /********************************************/
  class BSPContextImpl: public BSPContext, public DownwardProtocol {
  private:
    bool done;
    BSPJob* job;
    //string key;
    //const string* newKey;
    //const string* value;
    bool hasTask;
    //bool isNewKey;
    //bool isNewValue;
    string* inputKeyClass;
    string* inputValueClass;
    
    //string status;
    //float progressFloat;
    //uint64_t lastProgress;
    //bool statusSet;
      
    Protocol* protocol;
    UpwardProtocol *uplink;
    
    //string* inputSplit;
    
    RecordReader* reader;
    RecordWriter* writer;
      
    BSP* bsp;
    Partitioner* partitioner;
    
    const Factory* factory;
    pthread_mutex_t mutexDone;
    std::vector<int> registeredCounterIds;
      
    int32_t resultInt;
    bool isNewResultInt;  
    int64_t resultLong;
    bool isNewResultLong; 
    string resultString;
    bool isNewResultString;   
    vector<string> resultVector;
    bool isNewResultVector; 
    
    bool isNewKeyValuePair;  
    string currentKey;
    string currentValue;

  public:

    BSPContextImpl(const Factory& _factory) {
      //statusSet = false;
      done = false;
      //newKey = NULL;
      factory = &_factory;
      job = NULL;
        
      inputKeyClass = NULL;
      inputValueClass = NULL;
      
      //inputSplit = NULL;
      
      bsp = NULL;
      reader = NULL;
      writer = NULL;
      partitioner = NULL;
      protocol = NULL;
      //isNewKey = false;
      //isNewValue = false;
      //lastProgress = 0;
      //progressFloat = 0.0f;
      hasTask = false;
      pthread_mutex_init(&mutexDone, NULL);
        
      isNewResultInt = false;
      isNewResultString = false,
      isNewResultVector = false;
        
      isNewKeyValuePair = false;
    }

  
    /********************************************/
    /*********** DownwardProtocol IMPL **********/  
    /********************************************/
    virtual void start(int protocol) {
      if (protocol != 0) {
        throw Error("Protocol version " + toString(protocol) + 
                    " not supported");
      }
      partitioner = factory->createPartitioner(*this);
    }

    virtual void setBSPJob(vector<string> values) {
      int len = values.size();
      BSPJobImpl* result = new BSPJobImpl();
      HADOOP_ASSERT(len % 2 == 0, "Odd length of job conf values");
      for(int i=0; i < len; i += 2) {
        result->set(values[i], values[i+1]);
      }
      job = result;
    }

    virtual void setInputTypes(string keyType, string valueType) {
      inputKeyClass = new string(keyType);
      inputValueClass = new string(valueType);
    }
      
    virtual void setKeyValue(const string& _key, const string& _value) {
      currentKey = _key;
      currentValue = _value;
      isNewKeyValuePair = true;
    }
     
    /* private Method */
    void setupReaderWriter(bool pipedInput, bool pipedOutput) {
        
      if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::setupReaderWriter - pipedInput: %s pipedOutput: %s\n",
              (pipedInput)?"true":"false",(pipedOutput)?"true":"false");

      if (pipedInput && reader==NULL) {
        reader = factory->createRecordReader(*this);
        HADOOP_ASSERT((reader == NULL) == pipedInput,
                      pipedInput ? "RecordReader defined when not needed.":
                      "RecordReader not defined");
        
        //if (reader != NULL) {
        //    value = new string();
        //}
      }  
        
      if (pipedOutput && writer==NULL) {
        writer = factory->createRecordWriter(*this);
        HADOOP_ASSERT((writer == NULL) == pipedOutput,
                      pipedOutput ? "RecordWriter defined when not needed.":
                      "RecordWriter not defined");
      }
    }
      
    virtual void runSetup(bool pipedInput, bool pipedOutput) {
      setupReaderWriter(pipedInput,pipedOutput);
      
      if (bsp == NULL)  
        bsp = factory->createBSP(*this);
        
      if (bsp != NULL) {
        hasTask = true;
        bsp->setup(*this);
        hasTask = false;
        uplink->sendCMD(TASK_DONE);
      }
    }
      
    virtual void runBsp(bool pipedInput, bool pipedOutput) {
      setupReaderWriter(pipedInput,pipedOutput);

      if (bsp == NULL)  
          bsp = factory->createBSP(*this);

      if (bsp != NULL) {
        hasTask = true;
        bsp->bsp(*this);
        hasTask = false;
        uplink->sendCMD(TASK_DONE);
      }
    }
      
    virtual void runCleanup(bool pipedInput, bool pipedOutput) {
      setupReaderWriter(pipedInput,pipedOutput);
        
      if (bsp != NULL) {
        hasTask = true;
        bsp->cleanup(*this);
        hasTask = false;
        uplink->sendCMD(TASK_DONE);
      }
    }
      
    /********************************************/
    /*******       Partitioner            *******/  
    /********************************************/ 
    virtual void runPartition(const string& key, const string& value, int32_t numTasks){
      if (partitioner != NULL) {             
        int part = partitioner->partition(key, value, numTasks);
        uplink->sendCMD(PARTITION_RESPONSE, part);
      } else {
        if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::runPartition Partitioner is NULL!\n");
      }
    } 
                          
    virtual void setNewResult(int32_t _value) {
      resultInt = _value;
      isNewResultInt = true;  
    }

    virtual void setNewResult(int64_t _value) {
      resultLong = _value;
      isNewResultLong = true;  
    }
      
    virtual void setNewResult(const string& _value) {
      resultString = _value;
      isNewResultString = true;   
    }

    virtual void setNewResult(vector<string> _value) {
      resultVector = _value;
      isNewResultVector = true;    
    }

    virtual void close() {
      pthread_mutex_lock(&mutexDone);
      done = true;
      hasTask = false;
      pthread_mutex_unlock(&mutexDone);
      if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::close - done: %s hasTask: %s\n",
                (done)?"true":"false",(hasTask)?"true":"false");
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
      return job;
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
    /*
    virtual Counter* getCounter(const std::string& group, 
                                  const std::string& name) {
        int id = registeredCounterIds.size();
        registeredCounterIds.push_back(id);
        uplink->registerCounter(id, group, name);
        return new Counter(id);
    }*/
      
    /**
     * Increment the value of the counter with the given amount.
     */
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount)  {
        uplink->incrementCounter(group, name, amount); 
    }
      
    /********************************************/
    /************** BSPContext IMPL *************/  
    /********************************************/
      
    /**
     * Access the InputSplit of the bsp.
     */
    //virtual const string& getInputSplit() {
    //  return *inputSplit;
    //}
      
    /**
     * Get the name of the key class of the input to this task.
     */
    virtual const string& getInputKeyClass() {
      return *inputKeyClass;
    }

    /**
     * Get the name of the value class of the input to this task.
     */
    virtual const string& getInputValueClass() {
      return *inputValueClass;
    }

    /**
     * Send a data with a tag to another BSPSlave corresponding to hostname.
     * Messages sent by this method are not guaranteed to be received in a sent
     * order.
     */
    virtual void sendMessage(const string& peerName, const string& msg) {
        string values[] = {peerName, msg};
        uplink->sendCMD(SEND_MSG,values, 2);
    }
      
    /**
     * @return A message from the peer's received messages queue (a FIFO).
     */
    virtual const string& getCurrentMessage() {
      uplink->sendCMD(GET_MSG);
      
      while (!isNewResultString)
          protocol->nextEvent();
        
      isNewResultString = false;
      if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::getMessage - NewResultString: %s\n",resultString.c_str());
      return resultString;
    }

    /**
     * @return The number of messages in the peer's received messages queue.
     */
    virtual int getNumCurrentMessages() {
      uplink->sendCMD(GET_MSG_COUNT);
        
      while (!isNewResultInt)
        protocol->nextEvent();
      
      isNewResultInt = false;
      return resultInt;
    }
      
    /**
     * Barrier Synchronization.
     * 
     * Sends all the messages in the outgoing message queues to the corresponding
     * remote peers.
     */
    virtual void sync() {
      uplink->sendCMD(SYNC);
    }
    
    /**
     * @return the name of this peer in the format "hostname:port".
     */ 
    virtual const string& getPeerName() {
      uplink->sendCMD(GET_PEERNAME,-1);
    
      while (!isNewResultString)
        protocol->nextEvent();
    
      if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::getPeerName - NewResultString: %s\n",resultString.c_str());
      isNewResultString = false;
      return resultString;
    }
    
    /**
     * @return the name of n-th peer from sorted array by name.
     */
    virtual const string& getPeerName(int index) {
      uplink->sendCMD(GET_PEERNAME,index);
        
      while (!isNewResultString)
        protocol->nextEvent();
  
      if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::getPeerName - NewResultString: %s\n",resultString.c_str());
      isNewResultString = false;
      return resultString;
    }
    
    /**
     * @return the names of all the peers executing tasks from the same job
     *         (including this peer).
     */
    virtual vector<string> getAllPeerNames() {
      uplink->sendCMD(GET_ALL_PEERNAME);
        
      while (!isNewResultVector)
        protocol->nextEvent();
        
      isNewResultVector = false;
      return resultVector;
    }
    
    /**
     * @return the index of this peer from sorted array by name.
     */
    virtual int getPeerIndex() {
      uplink->sendCMD(GET_PEER_INDEX);
        
      while (!isNewResultInt)
        protocol->nextEvent();
        
      isNewResultInt = false;
      return resultInt;
    }
      
    /**
     * @return the number of peers
     */
    virtual int getNumPeers() {
      uplink->sendCMD(GET_PEER_COUNT);
        
      while (!isNewResultInt)
        protocol->nextEvent();
        
      isNewResultInt = false;
      return resultInt;       
    }
      
    /**
     * @return the count of current super-step
     */
    virtual long getSuperstepCount() {
      uplink->sendCMD(GET_SUPERSTEP_COUNT);
        
      while (!isNewResultLong)
        protocol->nextEvent();
        
      isNewResultLong = false;
      return resultLong;     
    }  
    
    /**
     * Clears all queues entries.
     */
    virtual void clear() {
      uplink->sendCMD(CLEAR);
    }

    /**
     * Writes a key/value pair to the output collector
     */
    virtual void write(const string& key, const string& value) {
        if (writer != NULL) {
            writer->emit(key, value);
        } else {
            string values[] = {key, value};
            uplink->sendCMD(WRITE_KEYVALUE, values, 2);
        }
    }
    
    /**
     * Deserializes the next input key value into the given objects;
     */
    virtual bool readNext(string& _key, string& _value) {
      uplink->sendCMD(READ_KEYVALUE);
        
      while (!isNewKeyValuePair)
        protocol->nextEvent();
      
      isNewKeyValuePair = false;
        
      _key = currentKey;
      _value = currentValue;
      
      if (logging && _key.empty() && _value.empty())  
        fprintf(stderr,"HamaPipes::BSPContextImpl::readNext - Empty KeyValuePair\n");
        
      return (!_key.empty() && !_value.empty());
    }
       
    /**
     * Closes the input and opens it right away, so that the file pointer is at
     * the beginning again.
     */
    virtual void reopenInput() {
      uplink->sendCMD(REOPEN_INPUT);
    }
      
      
    /********************************************/
    /*******  SequenceFileConnector IMPL  *******/  
    /********************************************/     
      
    /**
     * Open SequenceFile with opion "r" or "w"
     * @return the corresponding fileID
     */
    virtual int sequenceFileOpen(const string& path, const string& option, 
                                 const string& keyType, const string& valueType) {
      if (logging)  
        fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileOpen - Path: %s\n",path.c_str());
     
      if ( (option.compare("r")==0) || (option.compare("w")==0))  {
          
          string values[] = {path, option, keyType, valueType};
          uplink->sendCMD(SEQFILE_OPEN,values, 4);
      
          while (!isNewResultInt)
            protocol->nextEvent();
        
          isNewResultInt = false;
          return resultInt;
      } else { 
          fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileOpen wrong option: %s!\n",option.c_str());
          return -1; //Error wrong option
      }
    }

    /**
     * Read next key/value pair from the SequenceFile with fileID
     */
    virtual bool sequenceFileReadNext(int fileID, string& _key, string& _value) {
        
      uplink->sendCMD(SEQFILE_READNEXT,fileID);
        
      while (!isNewKeyValuePair)
        protocol->nextEvent();
        
      isNewKeyValuePair = false;
        
      _key = currentKey;
      _value = currentValue;
        
      if (logging && _key.empty() && _value.empty())  
        fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileReadNext - Empty KeyValuePair\n");
        
      return (!_key.empty() && !_value.empty());
    }

    /**
     * Append the next key/value pair to the SequenceFile with fileID
     */
    virtual bool sequenceFileAppend(int fileID, const string& key, const string& value) {
      string values[] = {key, value};
      uplink->sendCMD(SEQFILE_APPEND,fileID, values, 2);
                
      while (!isNewResultInt)
        protocol->nextEvent();
        
      isNewResultInt = false;
      return (resultInt==1);
    }

    /**
     * Close SequenceFile
     */
    virtual bool sequenceFileClose(int fileID) {
      uplink->sendCMD(SEQFILE_CLOSE,fileID);
        
      while (!isNewResultInt)
        protocol->nextEvent();
        
      if (logging && resultInt==0)  
        fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileClose - Nothing was closed!\n");
      else if (logging)
        fprintf(stderr,"HamaPipes::BSPContextImpl::sequenceFileClose - File was successfully closed!\n");
    
      isNewResultInt = false;
      return (resultInt==1);
    }
      
    /********************************************/
    /*************** Other STUFF  ***************/  
    /********************************************/
      
    void setProtocol(Protocol* _protocol, UpwardProtocol* _uplink) {
        protocol = _protocol;
        uplink = _uplink;
    }
   
    bool isDone() {
        pthread_mutex_lock(&mutexDone);
        bool doneCopy = done;
        pthread_mutex_unlock(&mutexDone);
        return doneCopy;
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
        while (!done && !hasTask) {		
            if(logging)fprintf(stderr,"HamaPipes::BSPContextImpl::waitForTask - done: %s hasTask: %s\n",
                    (done)?"true":"false",(hasTask)?"true":"false");
            protocol->nextEvent();
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
      if (reader) {
        reader->close();
      }
      
      if (bsp) {
        bsp->close();
      }
     
      if (writer) {
        writer->close();
      }
    }
      
    virtual ~BSPContextImpl() {
      delete job;
      delete inputKeyClass;
      delete inputValueClass;
      //delete inputSplit;
      //if (reader) {
      //  delete value;
      //}
      delete reader;
      delete bsp;
      delete writer;
      pthread_mutex_destroy(&mutexDone);
    }
  };

  /**
   * Ping the parent every 5 seconds to know if it is alive 
   */
  void* ping(void* ptr) {
    BSPContextImpl* context = (BSPContextImpl*) ptr;
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
          if(logging)fprintf(stderr,"HamaPipes::ping - connected to GroomServer Port: %s\n", portStr);   
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
  bool runTask(const Factory& factory) {
    try {
      HADOOP_ASSERT(getenv("hama.pipes.logging")!=NULL,"No environment found!");
        
      logging = (toInt(getenv("hama.pipes.logging"))==0)?false:true;  
      if(logging)fprintf(stderr,"HamaPipes::runTask - logging is: %s\n", (logging)?"true":"false"); 
        
      BSPContextImpl* context = new BSPContextImpl(factory);
      Protocol* connection;
        
      char* portStr = getenv("hama.pipes.command.port");
      int sock = -1;
      FILE* stream = NULL;
      FILE* outStream = NULL;
      char *bufin = NULL;
      char *bufout = NULL;
      if (portStr) {
        sock = socket(PF_INET, SOCK_STREAM, 0);
        HADOOP_ASSERT(sock != - 1,
                      string("problem creating socket: ") + strerror(errno));
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(toInt(portStr));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                      string("problem connecting command socket: ") +
                      strerror(errno));

        stream = fdopen(sock, "r");
        outStream = fdopen(sock, "w");

        // increase buffer size
        int bufsize = 128*1024;
        int setbuf;
        bufin = new char[bufsize];
        bufout = new char[bufsize];
        setbuf = setvbuf(stream, bufin, _IOFBF, bufsize);
        HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for inStream: ")
                                     + strerror(errno));
        setbuf = setvbuf(outStream, bufout, _IOFBF, bufsize);
        HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for outStream: ")
                                     + strerror(errno));
          
        connection = new BinaryProtocol(stream, context, outStream);
        if(logging)fprintf(stderr,"HamaPipes::runTask - connected to GroomServer Port: %s\n", portStr);  
          
      } else if (getenv("hama.pipes.command.file")) {
        char* filename = getenv("hama.pipes.command.file");
        string outFilename = filename;
        outFilename += ".out";
        stream = fopen(filename, "r");
        outStream = fopen(outFilename.c_str(), "w");
        connection = new BinaryProtocol(stream, context, outStream);
      } else {
        //connection = new TextProtocol(stdin, context, stdout);
        fprintf(stderr,"HamaPipes::runTask - Connection couldn't be initialized!\n");
        return -1;
      }
 
      context->setProtocol(connection, connection->getUplink());
        
      //pthread_t pingThread;
      //pthread_create(&pingThread, NULL, ping, (void*)(context));
      
      context->waitForTask();
        
      //while (!context->isDone()) {
        //context->nextKey();
      //}
        
      context->closeAll();
      connection->getUplink()->sendCMD(DONE);
      
      //pthread_join(pingThread,NULL);
      
      delete context;
      delete connection;
      if (stream != NULL) {
        fflush(stream);
      }
      if (outStream != NULL) {
        fflush(outStream);
      }
      fflush(stdout);
      if (sock != -1) {
        int result = shutdown(sock, SHUT_RDWR);
        HADOOP_ASSERT(result == 0, "problem shutting socket");
        result = close(sock);
        HADOOP_ASSERT(result == 0, "problem closing socket");
      }
      if (stream != NULL) {
        //fclose(stream);
      }
      if (outStream != NULL) {
        //fclose(outStream);
      } 
      delete bufin;
      delete bufout;
      return true;
    } catch (Error& err) {
      fprintf(stderr, "Hama Pipes Exception: %s\n", 
              err.getMessage().c_str());
      return false;
    }
  }
}

