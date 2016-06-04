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


# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   HAMA_GROOMS  File naming remote hosts.
#     Default is ${HAMA_CONF_DIR}/groomservers.
#   HAMA_CONF_DIR  Alternate conf dir. Default is ${HAMA_HOME}/conf.
#   HAMA_GROOM_SLEEP Seconds to sleep between spawning remote commands.
#   HAMA_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: grooms.sh [--config confdir] command..."

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
HOSTLIST=$HAMA_GROOMS

if [ -f "${HAMA_CONF_DIR}/hama-env.sh" ]; then
  . "${HAMA_CONF_DIR}/hama-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$HAMA_GROOMS" = "" ]; then
    export HOSTLIST="${HAMA_CONF_DIR}/groomservers"
  else
    export HOSTLIST="${HAMA_GROOMS}"
  fi
fi

for groom in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $HAMA_SSH_OPTS $groom $"${@// /\\ }" $groom \
   2>&1 | sed "s/^/$groom: /" &
 if [ "$HAMA_GROOM_SLEEP" != "" ]; then
   sleep $HAMA_GROOM_SLEEP
 fi
done

wait
