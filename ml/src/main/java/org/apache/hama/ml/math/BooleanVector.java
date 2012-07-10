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
package org.apache.hama.ml.math;

import java.util.Iterator;

/**
 * Vector consisting of booleans.
 */
public interface BooleanVector {

    /**
     * Gets the boolean at the given index.
     */
    public boolean get(int index);

    /**
     * Returns the length of this vector.
     */
    public int getLength();

    /**
     * Returns an array representation of the inner values.
     */
    public boolean[] toArray();

    /**
     * Iterates over the not-false elements in this vector.
     */
    public Iterator<BooleanVectorElement> iterateNonZero();

    /**
     * Element class for iterating.
     */
    public static final class BooleanVectorElement {

        private int index;
        private boolean value;

        public BooleanVectorElement() {
            super();
        }

        public BooleanVectorElement(int index, boolean value) {
            super();
            this.index = index;
            this.value = value;
        }

        /**
         * @return the index of the current element.
         */
        public final int getIndex() {
            return index;
        }

        /**
         * @return the value of the vector at the current index.
         */
        public final boolean getValue() {
            return value;
        }

        public final void setIndex(int in) {
            this.index = in;
        }

        public final void setValue(boolean in) {
            this.value = in;
        }
    }

}