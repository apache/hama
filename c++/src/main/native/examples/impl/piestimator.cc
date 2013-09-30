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

#include <time.h>
#include <math.h>
#include <stdlib.h>
#include <string>
#include <iostream>

using std::string;
using std::cout;

using HamaPipes::BSP;
using HamaPipes::BSPContext;
using namespace HadoopUtils;

class PiEstimatorBSP: public BSP {
private:
  string masterTask;
  long iterations; // iterations_per_bsp_task
public:
  PiEstimatorBSP(BSPContext& context) {
    iterations = 1000000L;
  }
  
  inline double closed_interval_rand(double x0, double x1) {
    return x0 + (x1 - x0) * rand() / ((double) RAND_MAX);
  }
  
  void setup(BSPContext& context) {
    // Choose one as a master
    masterTask = context.getPeerName(context.getNumPeers() / 2);
  }
  
  void bsp(BSPContext& context) {
    
    /* initialize random seed */
    srand(time(NULL));
    
    int in = 0;
    for (long i = 0; i < iterations; i++) {
      double x = 2.0 * closed_interval_rand(0, 1) - 1.0;
      double y = 2.0 * closed_interval_rand(0, 1) - 1.0;
      if (sqrt(x * x + y * y) < 1.0) {
        in++;
      }
    }
    
    context.sendMessage(masterTask, toString(in));
    context.sync();
  }
  
  void cleanup(BSPContext& context) {
    if (context.getPeerName().compare(masterTask)==0) {
      
      long totalHits = 0;
      int msgCount = context.getNumCurrentMessages();
      string received;
      for (int i=0; i<msgCount; i++) {
        string received = context.getCurrentMessage();
        totalHits += toInt(received);
      }
      
      double pi = 4.0 * totalHits / (msgCount * iterations);
      context.write("Estimated value of PI is", toString(pi));
    }
  }
};

int main(int argc, char *argv[]) {
  return HamaPipes::runTask(HamaPipes::TemplateFactory<PiEstimatorBSP>());
}

