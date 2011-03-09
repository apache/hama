#!/usr/bin/env bash

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


# Start hama map reduce daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hama-config.sh

# start bsp daemons
# start zookeeper first to minimize connection errors at startup
"$bin"/hama-daemons.sh --config "${HAMA_CONF_DIR}" start zookeeper
"$bin"/hama-daemon.sh --config $HAMA_CONF_DIR start bspmaster
"$bin"/hama-daemons.sh --config $HAMA_CONF_DIR start groom
