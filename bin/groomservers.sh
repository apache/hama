#!/usr/bin/env bash
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
# 
# Run a shell command on all regionserver hosts.
#
# Environment Variables
#
#   HAMA_GROOMSERVERS    File naming remote hosts.
#     Default is ${HADOOP_CONF_DIR}/regionservers
#   HADOOP_CONF_DIR  Alternate conf dir. Default is ${HADOOP_HOME}/conf.
#   HAMA_CONF_DIR  Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HADOOP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HADOOP_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HADOOP_HOME/bin/slaves.sh.

usage="Usage: groomservers [--config <hama-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hama-config.sh

# If the groomservers file is specified in the command line,
# then it takes precedence over the definition in 
# hama-env.sh. Save it here.
HOSTLIST=$HAMA_GROOMSERVERS

if [ -f "${HAMA_CONF_DIR}/hama-env.sh" ]; then
  . "${HAMA_CONF_DIR}/hama-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$HAMA_GROOMSERVERS" = "" ]; then
    export HOSTLIST="${HAMA_CONF_DIR}/groomservers"
  else
    export HOSTLIST="${HAMA_GROOMSERVERS}"
  fi
fi

for groomserver in `cat "$HOSTLIST"`; do
 ssh $HAMA_SSH_OPTS $groomserver $"${@// /\\ }" \
   2>&1 | sed "s/^/$groomserver: /" &
 if [ "$HAMA_SLAVE_SLEEP" != "" ]; then
   sleep $HAMA_SLAVE_SLEEP
 fi
done

wait
