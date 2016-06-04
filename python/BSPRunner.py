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

Main Runner utility that will get the bsp class from the user, passed via args and start
it with the whole context and stuff.

"""
import sys
from BSPPeer import BSPPeer

className = sys.argv[1]
module = __import__(className)
class_ = getattr(module, className)

bspInstance = class_()

peer = BSPPeer(bspInstance)
peer.runSetup()
peer.runBSP()
peer.runCleanup()
peer.done()