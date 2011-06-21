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
package org.apache.hama.examples.graph;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ShortestPathsGraphLoader {
  
  static Map<ShortestPathVertex, List<ShortestPathVertex>> loadGraph() {

    Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList = new HashMap<ShortestPathVertex, List<ShortestPathVertex>>();
    String[] cities = new String[] { "Frankfurt", "Mannheim", "Wuerzburg",
        "Stuttgart", "Kassel", "Karlsruhe", "Erfurt", "Nuernberg", "Augsburg",
        "Muenchen" };

    int id = 1;
    for (String city : cities) {
      if (city.equals("Frankfurt")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(85, "Mannheim"));
        list.add(new ShortestPathVertex(173, "Kassel"));
        list.add(new ShortestPathVertex(217, "Wuerzburg"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Stuttgart")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(183, "Nuernberg"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Kassel")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(502, "Muenchen"));
        list.add(new ShortestPathVertex(173, "Frankfurt"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Erfurt")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(186, "Wuerzburg"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Wuerzburg")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(217, "Frankfurt"));
        list.add(new ShortestPathVertex(168, "Erfurt"));
        list.add(new ShortestPathVertex(103, "Nuernberg"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Mannheim")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(80, "Karlsruhe"));
        list.add(new ShortestPathVertex(85, "Frankfurt"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Karlsruhe")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(250, "Augsburg"));
        list.add(new ShortestPathVertex(80, "Mannheim"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Augsburg")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(250, "Karlsruhe"));
        list.add(new ShortestPathVertex(84, "Muenchen"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Nuernberg")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(183, "Stuttgart"));
        list.add(new ShortestPathVertex(167, "Muenchen"));
        list.add(new ShortestPathVertex(103, "Wuerzburg"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      } else if (city.equals("Muenchen")) {
        List<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(167, "Nuernberg"));
        list.add(new ShortestPathVertex(173, "Kassel"));
        list.add(new ShortestPathVertex(84, "Augsburg"));
        adjacencyList.put(new ShortestPathVertex(0, city), list);
      }
      id++;
    }
    return adjacencyList;
  }

}
