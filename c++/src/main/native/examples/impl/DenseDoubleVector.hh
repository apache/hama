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

#include <string>

using std::string;

namespace math {
  
  class DenseDoubleVector {
  private:
    double *vector;
    int size;
  public:
    DenseDoubleVector(); // Default-Constructor
    DenseDoubleVector(int length);
    /// Creates a new vector with the given length and default value.
    DenseDoubleVector(int length, double val);
    // Creates a new vector with the given array.
    DenseDoubleVector(int length, double arr[]);
    DenseDoubleVector(const string values);
    ~DenseDoubleVector();  // Destructor
    
    int getLength();
    int getDimension();
    void set(int index, double value);
    double get(int index);
    
    DenseDoubleVector *add(DenseDoubleVector *v);
    DenseDoubleVector *add(double scalar);
    DenseDoubleVector *subtract(DenseDoubleVector *v);
    DenseDoubleVector *subtract(double v);
    DenseDoubleVector *subtractFrom(double v);
    
    DenseDoubleVector *multiply(double scalar);
    DenseDoubleVector *divide(double scalar);
    /*
     DenseDoubleVector *pow(int x);
     DenseDoubleVector *sqrt();
     */
    double sum();
    
    //DenseDoubleVector *abs();
    double dot(DenseDoubleVector *s);
    
    double max();
    int maxIndex();
    double min();
    int minIndex();
    
    double *toArray();
    virtual string toString();
  };
}

