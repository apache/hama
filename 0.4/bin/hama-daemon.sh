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


# Runs a Hama command as a daemon.
#
# Environment Variables
#
#   HAMA_CONF_DIR  Alternate conf dir. Default is ${HAMA_HOME}/conf.
#   HAMA_LOG_DIR   Where log files are stored.  PWD by default.
#   HAMA_MASTER    host:path where hama code should be rsync'd from
#   HAMA_PID_DIR   The pid files are stored. /tmp by default.
#   HAMA_IDENT_STRING   A string representing this instance of hama. $USER by default
#   HAMA_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: hama-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <hama-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hama-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

hama_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${HAMA_CONF_DIR}/hama-env.sh" ]; then
  . "${HAMA_CONF_DIR}/hama-env.sh"
fi

# get log directory
if [ "$HAMA_LOG_DIR" = "" ]; then
  export HAMA_LOG_DIR="$HAMA_HOME/logs"
fi
mkdir -p "$HAMA_LOG_DIR"

if [ "$HAMA_PID_DIR" = "" ]; then
  HAMA_PID_DIR=/tmp
fi

if [ "$HAMA_IDENT_STRING" = "" ]; then
  export HAMA_IDENT_STRING="$USER"
fi

# some variables
export HAMA_LOGFILE=hama-$HAMA_IDENT_STRING-$command-$HOSTNAME.log
export HAMA_ROOT_LOGGER="INFO,DRFA"
log=$HAMA_LOG_DIR/hama-$HAMA_IDENT_STRING-$command-$HOSTNAME.out
pid=$HAMA_PID_DIR/hama-$HAMA_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$HAMA_NICENESS" = "" ]; then
    export HAMA_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$HAMA_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$HAMA_MASTER" != "" ]; then
      echo rsync from $HAMA_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $HAMA_MASTER/ "$HAMA_HOME"
    fi

    hama_rotate_log $log
    echo starting $command, logging to $log
    cd "$HAMA_HOME"
    nohup nice -n $HAMA_NICENESS "$HAMA_HOME"/bin/hama --config $HAMA_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
