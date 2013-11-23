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
  
  /* Generic template factory specification */
  template <class BSP, class K1, class V1, class K2, class V2, class M>
  class TemplateFactory2: public Factory<K1, V1, K2, V2, M> {
  public:
    BSP* createBSP(BSPContext<K1, V1, K2, V2, M>& context) const {
      return new BSP(context);
    }
  };
  
  /* Template factory including partitioner specification */
  template <class BSP, class K1, class V1, class K2, class V2, class M, class partitioner=void>
  class TemplateFactory: public TemplateFactory2<BSP, K1, V1, K2, V2, M> {
  public:
    partitioner* createPartitioner(BSPContext<K1, V1, K2, V2, M>& context) const {
      return new partitioner(context);
    }
  };
  
  /* Template factory without partitioner specification */
  template <class BSP, class K1, class V1, class K2, class V2, class M>
  class TemplateFactory<BSP, K1, V1, K2, V2, M, void>
  : public TemplateFactory2<BSP, K1, V1, K2, V2, M> {
  };
  
}

#endif
