# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

Python implementation of the K-Means clustering example with BSP.
According presentation: http://www.slideshare.net/tjungblut/kmeans-with-bsp

"""
from BSP import BSP
from math import sqrt

class KMeansBSP(BSP):
    # read the first n clusters
    def setup(self, peer):
        self.numCenters = int(peer.config.get("kmeans.num.centers"))
        self.maxIterations = -1
        if not peer.config.get("kmeans.max.iterations"):
            self.maxIterations = int(peer.config.get("kmeans.max.iterations"))
        self.centers = []

        for i in range(self.numCenters):
            val = peer.readNext()
            self.centers.append(self.toVector(val[1])) # the value (index 1) is the text line
        peer.reopenInput()

    def bsp(self, peer):
        while True:
            self.assignCenters(peer)
            peer.sync()
            converged = self.updateCenters(peer)
            peer.reopenInput()
            if converged == 0:
                break
                # cool simplification of maxIterations > 0 && maxIterations < peer.getSuperstepCount()
            if 0 < self.maxIterations < peer.getSuperstepCount():
                break

        self.recalculateAssignmentsAndWrite(peer)

    def assignCenters(self, peer):
        newCenterArray = [None] * self.numCenters
        summationCount = [None] * self.numCenters

        while True:
            line = peer.readNext()
            if not line: break
            self.assignCentersInternal(newCenterArray, summationCount, self.toVector(line[1]))

        # now send messages about the local updates to each other peer
        for i in range(self.numCenters):
            if newCenterArray[i] is not None:
                for peerName in peer.getAllPeerNames():
                    # send message: "i_sum_vector" where i is the index in centers,
                    # sum is the summation count and the rest is the vector, everything is space separated
                    peer.send(peerName,
                        "%s %s %s" % (str(i), str(summationCount[i]), " ".join(map(str, newCenterArray[i]))))

    def getNearestCenter(self, vector):
        lowestIndex = 0
        lowest = float("inf")
        for i in range(self.numCenters):
            dist = self.distance(vector, self.centers[i])
            if lowest > dist:
                lowest = dist
                lowestIndex = i
        return lowestIndex


    def assignCentersInternal(self, newCenterArray, summationCount, vector):
        lowestCenterIndex = self.getNearestCenter(vector)
        center = self.centers[lowestCenterIndex]
        if newCenterArray[lowestCenterIndex] is None:
            newCenterArray[lowestCenterIndex] = center
            summationCount[lowestCenterIndex] = 0
        else:
            newCenterArray[lowestCenterIndex] = self.sum(newCenterArray[lowestCenterIndex], vector)
            summationCount[lowestCenterIndex] += 1


    def updateCenters(self, peer):
        msgCenters = [None] * self.numCenters
        incrementSum = [None] * self.numCenters
        self.fill(incrementSum, 0)

        for msg in peer.getAllMessages():
            split = msg.split()
            centerIndex = int(split[0])
            oldCenter = msgCenters[centerIndex]
            incrementSum[centerIndex] += int(split[1])
            newCenter = self.toVector(" ".join(split[2:])) # join the vector part back to string
            if oldCenter is None:
                msgCenters[centerIndex] = newCenter
            else:
                msgCenters[centerIndex] = self.sum(oldCenter, newCenter)

        # update and convergence checks
        for i in range(self.numCenters):
            if msgCenters[i] is not None:
                msgCenters[i] = [x / incrementSum[i] for x in msgCenters[i]] # average the center messages
                # finally check for convergence by the absolute difference
        convergedCounter = 0;
        for i in range(self.numCenters):
            oldCenter = self.centers[i]
            if msgCenters[i] is not None:
                calculateError = self.subtractAbsSum(oldCenter, msgCenters[i])
                if calculateError > 0.0:
                    self.centers[i] = msgCenters[i]
                    convergedCounter += 1

        return convergedCounter


    def recalculateAssignmentsAndWrite(self, peer):
        # write the centers first
        for center in self.centers:
            peer.write(center, "")
        peer.write("\n", "")

        while True:
            line = peer.readNext()
            if not line: break
            lowestDistantCenter = self.getNearestCenter(self.toVector(line[1]))
            peer.write(str(lowestDistantCenter), line[1])

        pass

    def fill(self, incrementSum, x):
        for i in range(len(incrementSum)):
            incrementSum[i] = x

    def sum(self, vector, vector2):
        vec = []
        for i in range(len(vector)):
            vec.append(vector[i] + vector2[i])
        return vec

    def subtractAbsSum(self, vector, vector2):
        diff = 0
        for i in range(len(vector)):
            diff += abs(vector[i] - vector2[i])
        return diff

    # distance between two vectors
    def distance(self, a, b):
        sum = 0
        for i in range(len(a)):
            diff = b[i] - a[i]
            sum += (diff * diff)
        return sqrt(sum)


    # splits a line by space and puts it into a vector (list)
    def toVector(self, line):
        vec = []
        split = line.split()
        for part in split:
            vec.append(float(part))
        return vec