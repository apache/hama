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

# included in all the hama scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the Hama installation
export HAMA_HOME=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      HAMA_CONF_DIR=$confdir
    fi
fi
 
# Allow alternate conf dir location.
HAMA_CONF_DIR="${HAMA_CONF_DIR:-$HAMA_HOME/conf}"

# Source the hama-env.sh.  
# Will have JAVA_HOME and HAMA_MANAGES_ZK defined.
if [ -f "${HAMA_CONF_DIR}/hama-env.sh" ]; then
  . "${HAMA_CONF_DIR}/hama-env.sh"
fi

#check to see it is specified whether to use the grooms or the
# masters file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        grooms=$1
        shift
        export HAMA_SLAVES="${HAMA_CONF_DIR}/$grooms"
    fi
fi
