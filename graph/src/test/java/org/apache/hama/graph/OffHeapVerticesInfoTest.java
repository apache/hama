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
package org.apache.hama.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Testcase for {@link OffHeapVerticesInfo}
 */
public class OffHeapVerticesInfoTest {

    private Random random;

    @Before
    public void setUp() throws Exception {
        random = new Random();
    }

    @Test
    public void testAdditionWithDefaultStrictCache() throws Exception {
        OffHeapVerticesInfo<DoubleWritable, DoubleWritable, DoubleWritable> verticesInfo =
                new OffHeapVerticesInfo<DoubleWritable, DoubleWritable, DoubleWritable>();
        Vertex<DoubleWritable, DoubleWritable, DoubleWritable> vertex = creteDoubleVertex(1d);
        assertNotNull(vertex.getVertexID());
        verticesInfo.addVertex(vertex);
        assertTrue("added vertex could not be found in the cache", verticesInfo.iterator().hasNext());
    }

    @Test
    @Ignore("failing")
    public void testAdditionWithNonStrictCache() throws Exception {
        OffHeapVerticesInfo<DoubleWritable, DoubleWritable, DoubleWritable> verticesInfo =
                new OffHeapVerticesInfo<DoubleWritable, DoubleWritable, DoubleWritable>(false);
        Vertex<DoubleWritable, DoubleWritable, DoubleWritable> vertex = creteDoubleVertex(2d);
        verticesInfo.addVertex(vertex);
        assertTrue("added vertex could not be found in the cache", verticesInfo.iterator().hasNext());
    }


    private Vertex<DoubleWritable, DoubleWritable, DoubleWritable> creteDoubleVertex(final Double id) {
        return new Vertex<DoubleWritable, DoubleWritable, DoubleWritable>() {

            @Override
            public DoubleWritable getVertexID() {
                return new DoubleWritable(id);
            }

            @Override
            public DoubleWritable createVertexIDObject() {
                return new DoubleWritable(id);
            }

            @Override
            public DoubleWritable createEdgeCostObject() {
                return new DoubleWritable(random.nextDouble());
            }

            @Override
            public DoubleWritable createVertexValue() {
                return new DoubleWritable(random.nextDouble());
            }

            @Override
            public void readState(DataInput in) throws IOException {
            }

            @Override
            public void writeState(DataOutput out) throws IOException {
            }

            @Override
            public void compute(Iterator<DoubleWritable> messages) throws IOException {
            }
        };
    }
}
