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
#include "hama/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "DenseDoubleVector.hh"

#include <time.h>
#include <math.h>
#include <string>
#include <string>
#include <iostream>
#include <sstream>

using std::string;
using std::cout;

using HamaPipes::BSP;
using HamaPipes::BSPJob;
using HamaPipes::Partitioner;
using HamaPipes::BSPContext;
using namespace HadoopUtils;

using math::DenseDoubleVector;

class MatrixMultiplicationBSP: public BSP {
private:
  string masterTask;
  int seqFileID;
  string HAMA_MAT_MULT_B_PATH;
public:
  MatrixMultiplicationBSP(BSPContext& context) {
    seqFileID = 0;
    HAMA_MAT_MULT_B_PATH = "hama.mat.mult.B.path";
  }
  
  void setup(BSPContext& context) {
    // Choose one as a master
    masterTask = context.getPeerName(context.getNumPeers() / 2);
    
    reopenMatrixB(context);
  }
  
  void bsp(BSPContext& context) {
    
    string aRowKey;
    string aRowVectorStr;
    // while for each row of matrix A
    while(context.readNext(aRowKey, aRowVectorStr)) {
      
      DenseDoubleVector *aRowVector = new DenseDoubleVector(aRowVectorStr);
      DenseDoubleVector *colValues = NULL;
      
      string bColKey;
      string bColVectorStr;
      
      // while for each col of matrix B
      while (context.sequenceFileReadNext(seqFileID,bColKey,bColVectorStr)) {
        
        DenseDoubleVector *bColVector = new DenseDoubleVector(bColVectorStr);
        
        if (colValues == NULL)
          colValues = new DenseDoubleVector(bColVector->getDimension());
        
        double dot = aRowVector->dot(bColVector);
        
        colValues->set(toInt(bColKey), dot);
      }
      
      // Submit one calculated row
      std::stringstream message;
      message << aRowKey << ":" << colValues->toString();
      context.sendMessage(masterTask, message.str());
      
      reopenMatrixB(context);
    }
    
    context.sequenceFileClose(seqFileID);
    context.sync();
  }
  
  void cleanup(BSPContext& context) {
    if (context.getPeerName().compare(masterTask)==0) {
      
      int msgCount = context.getNumCurrentMessages();
      
      for (int i=0; i<msgCount; i++) {
        
        string received = context.getCurrentMessage();
        //key:value1,value2,value3
        int pos = (int)received.find(":");
        string key = received.substr(0,pos);
        string values = received.substr(pos+1,received.length());
        
        context.write(key, values);
      }
    }
  }
  
  void reopenMatrixB(BSPContext& context) {
    if (seqFileID!=0) {
      context.sequenceFileClose(seqFileID);
    }
    
    const BSPJob* job = context.getBSPJob();
    string path = job->get(HAMA_MAT_MULT_B_PATH);
    
    seqFileID = context.sequenceFileOpen(path,"r",
                                         "org.apache.hadoop.io.IntWritable",
                                         "org.apache.hama.ml.writable.VectorWritable");
  }
  
};

class MatrixRowPartitioner: public Partitioner {
public:
  MatrixRowPartitioner(BSPContext& context) { }
  
  int partition(const string& key,const string& value, int32_t numTasks) {
    return toInt(key) % numTasks;
  }
};

int main(int argc, char *argv[]) {
  return HamaPipes::runTask(HamaPipes::TemplateFactory<MatrixMultiplicationBSP,MatrixRowPartitioner>());
}
