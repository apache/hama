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
#include "hadoop/Splitter.hh"

#include <string>
#include <vector>

namespace HadoopUtils {


  Splitter::Splitter ( const std::string& src, const std::string& delim ) {
    reset ( src, delim );
  }
    
  std::string& Splitter::operator[] ( size_type i ) {
    return _tokens.at ( i );
  }
        
  Splitter::size_type Splitter::size() {
    return _tokens.size();
  }
        
  void Splitter::reset ( const std::string& src, const std::string& delim ) {
    std::vector<std::string> tokens;
    std::string::size_type start = 0;
    std::string::size_type end;
            
    for ( ; ; ) {
      end = src.find ( delim, start );
      tokens.push_back ( src.substr ( start, end - start ) );
            
      // We just copied the last token
      if ( end == std::string::npos )
        break;
                
        // Exclude the delimiter in the next search
        start = end + delim.size();
    }
            
    _tokens.swap ( tokens );
  }

}
