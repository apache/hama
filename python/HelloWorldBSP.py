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

Basic Hello World BSP, in Hama this is called serialize printing.
Each task sends its peer name to each other task who reads the
message and outputs it to console.

"""
from BSP import BSP

class HelloWorldBSP(BSP):
    def bsp(self, peer):
        name = peer.getPeerName()
        for i in range(15):
            for otherPeer in peer.getAllPeerNames():
                peer.send(otherPeer, ("Hello from " + name + " in superstep " + str(i)))
            peer.sync()
            for msg in peer.getAllMessages():
                peer.write(msg,"")


