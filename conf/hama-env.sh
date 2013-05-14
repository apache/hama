#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Set environment variables here.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/jvm/java-7-oracle

# Where log files are stored.  $HAMA_HOME/logs by default.
# export HAMA_LOG_DIR=${HAMA_HOME}/logs

# The maximum amount of heap to use, in MB. Default is 1000.
# export HAMA_HEAPSIZE=1000

# Extra ssh options.  Empty by default.
# export HAMA_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HAMA_CONF_DIR"

# Tell Hama whether it should manage it's own instance of Zookeeper or not.
# export HAMA_MANAGES_ZK=true 
