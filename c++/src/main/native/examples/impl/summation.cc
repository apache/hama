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

#include <stdlib.h>
#include <string>
#include <iostream>

using std::string;

using HamaPipes::BSP;
using HamaPipes::BSPContext;

class SummationBSP: public BSP {
private:
  string masterTask;
public:
  SummationBSP(BSPContext& context) {  }
  
  void setup(BSPContext& context) {
    // Choose one as a master
    masterTask = context.getPeerName(context.getNumPeers() / 2);
  }
  
  void bsp(BSPContext& context) {
    
    double intermediateSum = 0.0;
    string key;
    string value;
    
    while(context.readNext(key,value)) {
      intermediateSum += HadoopUtils::toDouble(value);
    }
    
    context.sendMessage(masterTask, HadoopUtils::toString(intermediateSum));
    context.sync();
  }
  
  void cleanup(BSPContext& context) {
    if (context.getPeerName().compare(masterTask)==0) {
      
      double sum = 0.0;
      int msgCount = context.getNumCurrentMessages();
      for (int i=0; i<msgCount; i++) {
        string received = context.getCurrentMessage();
        sum += HadoopUtils::toDouble(received);
      }
      context.write("Sum", HadoopUtils::toString(sum));
    }
  }
};

int main(int argc, char *argv[]) {
  return HamaPipes::runTask(HamaPipes::TemplateFactory<SummationBSP>());
}

