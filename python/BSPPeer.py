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

The BSPPeer handles the incoming protocol requests and forwards it to the BSP class.
Basically you can register the to be executed BSP class into this peer,
it will then callback the according methods.

"""
from BspJobConfiguration import BspJobConfiguration
from sys import stdout, stdin
from BinaryProtocol import BinaryProtocol as bp

class BSPPeer:
    PROTOCOL_VERSION = 0

    def __init__(self, bspClass):
        self.config = BspJobConfiguration()
        self.bspClass = bspClass;
        self.initialize()

    def initialize(self):
        """
        INIT protocol works as follows:
        START OP_CODE
        PROTOCOL_NUMBER
        SET_BSPJOB_CONF OP_CODE
        NUMBER OF CONF ITEMS (#KEY + #VALUES)
        N-LINES, where line is key and the following the value
        """
        # parse our initial values
        line = readLine()
        # start code is the first
        if line == bp.getProtocolString(bp.START):
            # check the protocol compatibility
            protocolNumber = int(readLine())
            if protocolNumber != self.PROTOCOL_VERSION:
                raise RuntimeError(
                    "Protocol version mismatch: Expected: " + str(self.PROTOCOL_VERSION) +
                    " but got: " + str(protocolNumber))
        line = readLine()
        # parse the configurations
        if line == bp.getProtocolString(bp.SET_BSPJOB_CONF):
            numberOfItems = readLine()
            key = None
            value = None
            for i in range(0, int(numberOfItems), 2):
                key = readLine()
                value = readLine()
                self.config.put(key, value)

        self.ack(bp.START)

    def send(self, peer, msg):
        println(bp.getProtocolString(bp.SEND_MSG))
        println(peer)
        println(msg)

    def getCurrentMessage(self):
        println(bp.getProtocolString(bp.GET_MSG))
        line = readLine()
        # if no message is send it will send %%-1%%
        if line == "%%-1%%":
            return False

        return line;

    def getAllMessages(self):
        msgs = []
        numMessages = self.getNumCurrentMessages()
        for i in range(int(numMessages)):
            msgs.append(self.getCurrentMessage())
        return msgs

    def getNumCurrentMessages(self):
        println(bp.getProtocolString(bp.GET_MSG_COUNT))
        return readLine()

    def sync(self):
        println(bp.getProtocolString(bp.SYNC))
        # this should block now until we get a response
        line = readLine()
        if line != (bp.getProtocolString(bp.SYNC) + "_SUCCESS"):
            raise RuntimeError(
                "Barrier sync failed!")

    def getSuperstepCount(self):
        println(bp.getProtocolString(bp.GET_SUPERSTEP_COUNT))
        return readLine()

    def getPeerName(self):
        return self.getPeerNameForIndex(-1)

    def getPeerNameForIndex(self, index):
        println(bp.getProtocolString(bp.GET_PEERNAME))
        println(str(index));
        return readLine()

    def getPeerIndex(self):
        println(bp.getProtocolString(bp.GET_PEER_INDEX))
        return readLine()

    def getAllPeerNames(self):
        println(bp.getProtocolString(bp.GET_ALL_PEERNAME))
        ln = readLine()
        names = []
        for i in range(int(ln)):
            peerName = readLine()
            names.append(peerName)
        return names

    def getNumPeers(self):
        println(bp.getProtocolString(bp.GET_PEER_COUNT))
        return readLine()

    def clear(self):
        println(bp.getProtocolString(bp.CLEAR))

    def write(self, key, value):
        println(bp.getProtocolString(bp.WRITE_KEYVALUE))
        println(key)
        println(value)

    def readNext(self):
        println(bp.getProtocolString(bp.READ_KEYVALUE))
        line = readLine()
        secondLine = readLine()
        # if no message is send it will send %%-1%%
        if line == "%%-1%%" and secondLine == "%%-1%%":
            return False
        return [line, secondLine]

    def reopenInput(self):
        println(bp.getProtocolString(bp.REOPEN_INPUT))

    # TODO counters, seq writes and partitioning

    def runSetup(self):
        line = readLine()
        # start code is the first
        if line.startswith(bp.getProtocolString(bp.RUN_SETUP)):
            self.bspClass.setup(self);
            self.ack(bp.RUN_SETUP)

    def runBSP(self):
        line = readLine()
        # start code is the first
        if line.startswith(bp.getProtocolString(bp.RUN_BSP)):
            self.bspClass.bsp(self);
            self.ack(bp.RUN_BSP)

    def runCleanup(self):
        line = readLine()
        # start code is the first
        if line.startswith(bp.getProtocolString(bp.RUN_CLEANUP)):
            self.bspClass.cleanup(self);
            self.ack(bp.RUN_CLEANUP)

    def ack(self, code):
        println(bp.getAckProtocolString(code))

    def done(self):
        println(bp.getProtocolString(bp.TASK_DONE))
        println(bp.getProtocolString(bp.DONE))

    def log(self, msg):
        println(bp.getProtocolString(bp.LOG) + msg)


def readLine():
    return stdin.readline().rstrip('\n')


def println(text):
    print(text)
    stdout.flush()
