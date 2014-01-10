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

#include <time.h>
#include <math.h>
#include <stdlib.h>
#include <string>
#include <iostream>

using std::string;

using HamaPipes::BSP;
using HamaPipes::BSPContext;

class PiEstimatorBSP: public BSP<void,void,void,double,int> {
private:
  string master_task_;
  long iterations_; // iterations_per_bsp_task
  
public:
  PiEstimatorBSP(BSPContext<void,void,void,double,int>& context) {
    iterations_ = 10000000L;
  }
  
  inline double closed_interval_rand(double x0, double x1) {
    return x0 + (x1 - x0) * rand() / ((double) RAND_MAX);
  }
  
  void setup(BSPContext<void,void,void,double,int>& context) {
    // Choose one as a master
    master_task_ = context.getPeerName(context.getNumPeers() / 2);
  }
  
  void bsp(BSPContext<void,void,void,double,int>& context) {
    
    /* initialize random seed */
    srand(time(NULL));
    
    int in = 0;
    for (long i = 0; i < iterations_; i++) {
      double x = 2.0 * closed_interval_rand(0, 1) - 1.0;
      double y = 2.0 * closed_interval_rand(0, 1) - 1.0;
      if (sqrt(x * x + y * y) < 1.0) {
        in++;
      }
    }
    
    context.sendMessage(master_task_, in);
    context.sync();
  }
  
  void cleanup(BSPContext<void,void,void,double,int>& context) {
    if (context.getPeerName().compare(master_task_)==0) {
      
      long total_hits = 0;
      int msg_count = context.getNumCurrentMessages();
      for (int i=0; i < msg_count; i++) {
        total_hits += context.getCurrentMessage();
      }
      
      double pi = 4.0 * total_hits / (msg_count * iterations_);
      context.write(pi);
    }
  }
};

int main(int argc, char *argv[]) {
  return HamaPipes::runTask<void,void,void,double,int>(HamaPipes::TemplateFactory<PiEstimatorBSP,void,void,void,double,int>());
}

