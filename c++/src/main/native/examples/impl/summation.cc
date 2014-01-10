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

class SummationBSP: public BSP<string,string,void,double,double> {
private:
  string master_task_;
  
public:
  SummationBSP(BSPContext<string,string,void,double,double>& context) {  }
  
  void setup(BSPContext<string,string,void,double,double>& context) {
    // Choose one as a master
    master_task_ = context.getPeerName(context.getNumPeers() / 2);
  }
  
  void bsp(BSPContext<string,string,void,double,double>& context) {
    
    double intermediate_sum = 0.0;
    string key;
    string value;
    
    while(context.readNext(key,value)) {
      // We are using the KeyValueTextInputFormat,
      // therefore we have to convert string value to double
      intermediate_sum += HadoopUtils::toDouble(value);
    }
    
    context.sendMessage(master_task_, intermediate_sum);
    context.sync();
  }
  
  void cleanup(BSPContext<string,string,void,double,double>& context) {
    if (context.getPeerName().compare(master_task_)==0) {
      
      double sum = 0.0;
      int msg_count = context.getNumCurrentMessages();
      for (int i=0; i < msg_count; i++) {
        sum += context.getCurrentMessage();
      }
      context.write(sum);
    }
  }
};

int main(int argc, char *argv[]) {
  return HamaPipes::runTask<string,string,void,double,double>(HamaPipes::TemplateFactory<SummationBSP,string,string,void,double,double>());
}

