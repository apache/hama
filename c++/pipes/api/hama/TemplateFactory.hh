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
#ifndef HAMA_PIPES_TEMPLATE_FACTORY_HH
#define HAMA_PIPES_TEMPLATE_FACTORY_HH

namespace HamaPipes {

  template <class BSP>
  class TemplateFactory2: public Factory {
  public:
    BSP* createBSP(BSPContext& context) const {
      return new BSP(context);
    }
  };

  template <class BSP, class partitioner=void>
  class TemplateFactory: public TemplateFactory2<BSP> {
  public:
      partitioner* createPartitioner(BSPContext& context) const {
          return new partitioner(context);
      }
  };
  template <class BSP>
  class TemplateFactory<BSP,void>
      : public TemplateFactory2<BSP> {
  };
    
}

#endif
