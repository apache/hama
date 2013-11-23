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

#include "hadoop/StringUtils.hh"
#include "hadoop/Splitter.hh"
#include "DenseDoubleVector.hh"

#include <limits>
#include <vector>
#include <stdlib.h>
#include <string>
#include <iostream>

using std::string;
using std::cout;
using HadoopUtils::Splitter;

namespace math {
  
  DenseDoubleVector::DenseDoubleVector(int len) : vector(new double[len]), size(len) {
  }
  
  DenseDoubleVector::DenseDoubleVector(int len, double val) : vector(new double[len]), size(len) {
    for (int i=0; i<len; i++)
      vector[i] = val;
  }
  
  DenseDoubleVector::DenseDoubleVector(int len, double arr[]) : vector(arr), size(len) {
  }

  DenseDoubleVector::DenseDoubleVector(const string values) {
    
    Splitter split ( values, "," );
    size = split.size();
    
    vector = new double[size];
    for ( Splitter::size_type i = 0; i < split.size(); i++ ) {
      vector[i] = HadoopUtils::toDouble(split[i]);
    }
  }
  
  DenseDoubleVector::~DenseDoubleVector() {
    free(vector);
  }
  
  int DenseDoubleVector::getLength() {
    return size;
  }
  
  int DenseDoubleVector::getDimension() {
    return getLength();
  }
  
  void DenseDoubleVector::set(int index, double value) {
    vector[index] = value;
  }
  
  double DenseDoubleVector::get(int index) {
    return vector[index];
  }
  
  DenseDoubleVector* DenseDoubleVector::add(DenseDoubleVector *v) {
    DenseDoubleVector *newv = new DenseDoubleVector(v->getLength());
    for (int i = 0; i < v->getLength(); i++) {
      newv->set(i, this->get(i) + v->get(i));
    }
    return newv;
  }
  
  DenseDoubleVector* DenseDoubleVector::add(double scalar) {
    DenseDoubleVector *newv = new DenseDoubleVector(this->getLength());
    for (int i = 0; i < this->getLength(); i++) {
      newv->set(i, this->get(i) + scalar);
    }
    return newv;
  }
  
  DenseDoubleVector* DenseDoubleVector::subtract(DenseDoubleVector *v) {
    DenseDoubleVector *newv = new DenseDoubleVector(v->getLength());
    for (int i = 0; i < v->getLength(); i++) {
      newv->set(i, this->get(i) - v->get(i));
    }
    return newv;
  }
  
  DenseDoubleVector* DenseDoubleVector::subtract(double v) {
    DenseDoubleVector *newv = new DenseDoubleVector(size);
    for (int i = 0; i < size; i++) {
      newv->set(i, vector[i] - v);
    }
    return newv;
  }
  
  DenseDoubleVector* DenseDoubleVector::subtractFrom(double v) {
    DenseDoubleVector *newv = new DenseDoubleVector(size);
    for (int i = 0; i < size; i++) {
      newv->set(i, v - vector[i]);
    }
    return newv;
  }
  
  DenseDoubleVector* DenseDoubleVector::multiply(double scalar) {
    DenseDoubleVector *v = new DenseDoubleVector(size);
    for (int i = 0; i < size; i++) {
      v->set(i, this->get(i) * scalar);
    }
    return v;
  }
  
  DenseDoubleVector* DenseDoubleVector::divide(double scalar) {
    DenseDoubleVector *v = new DenseDoubleVector(size);
    for (int i = 0; i < size; i++) {
      v->set(i, this->get(i) / scalar);
    }
    return v;
  }
  
  /*
   DenseDoubleVector* pow(int x) {
   DenseDoubleVector *v = new DenseDoubleVector(size);
   for (int i = 0; i < size; i++) {
   double value = 0.0;
   // it is faster to multiply when we having ^2
   if (x == 2) {
   value = vector[i] * vector[i];
   }
   else {
   value = pow(vector[i], x);
   }
   v->set(i, value);
   }
   return v;
   }
   
   DenseDoubleVector* sqrt() {
   DenseDoubleVector *v = new DenseDoubleVector(size);
   for (int i = 0; i < size; i++) {
   v->set(i, sqrt(vector[i]));
   }
   return v;
   }
   */
  
  double DenseDoubleVector::sum() {
    double sum = 0.0;
    for (int i = 0; i < size; i++) {
      sum +=  vector[i];
    }
    return sum;
  }
  
  /*
   DenseDoubleVector* abs() {
   DenseDoubleVector *v = new DenseDoubleVector(size);
   for (int i = 0; i < size; i++) {
   v->set(i, abs(vector[i]));
   }
   return v;
   }
   */
  
  double DenseDoubleVector::dot(DenseDoubleVector *s) {
    double dotProduct = 0.0;
    for (int i = 0; i < size; i++) {
      dotProduct += this->get(i) * s->get(i);
    }
    return dotProduct;
  }
  
  double DenseDoubleVector::max() {
    double max = std::numeric_limits<double>::min();
    for (int i = 0; i < size; i++) {
      double d = vector[i];
      if (d > max) {
        max = d;
      }
    }
    return max;
  }
  
  int DenseDoubleVector::maxIndex() {
    double max = std::numeric_limits<double>::min();
    int maxIndex = 0;
    for (int i = 0; i < size; i++) {
      double d = vector[i];
      if (d > max) {
        max = d;
        maxIndex = i;
      }
    }
    return maxIndex;
  }
  
  double DenseDoubleVector::min() {
    double min = std::numeric_limits<double>::max();
    for (int i = 0; i < size; i++) {
      double d = vector[i];
      if (d < min) {
        min = d;
      }
    }
    return min;
  }
  
  int DenseDoubleVector::minIndex() {
    double min = std::numeric_limits<double>::max();
    int minIndex = 0;
    for (int i = 0; i < size; i++) {
      double d = vector[i];
      if (d < min) {
        min = d;
        minIndex = i;
      }
    }
    return minIndex;
  }
  
  double* DenseDoubleVector::toArray() {
    return vector;
  }
  
  string DenseDoubleVector::toString() {
    string str;
    string delimiter = ",";
    for (int i = 0; i < size; i++)
      if (i==0)
        str += HadoopUtils::toString(vector[i]);
      else
        str += delimiter + HadoopUtils::toString(vector[i]);
    
    return str;
  }
}

