#! /bin/bash

BASE_DIR=$(dirname $0)/..
CLASSPATH=$(echo $BASE_DIR/lib/*.jar | tr ' ' ':')

# get java version
JAVA="$JAVA_HOME/bin/java"

JAVA_VERSION=$($JAVA -version 2>&1 | awk -F\" '/version/{print $2}')
echo "java version:$JAVA_VERSION path:$JAVA"

MEMORY=$(cat /proc/meminfo | grep 'MemTotal' | awk -F : '{print $2}' | awk '{print $1}' | sed 's/^[ \t]*//g')
echo -e "Total Memory:\n${MEMORY} KB\n"

JAVA_OPT_1="-server -Xms2000m -Xmx2000m -Xmn1000m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=1024m "
JAVA_OPT_2="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC"

JAVA_OPTS="${JAVA_OPT_1} ${JAVA_OPT_2}"

JAVA_CONFIG=$(mktemp XXXXXXXX)
cat <<EOF | xargs echo >$JAVA_CONFIG
${JAVA_OPTS}
-cp $CLASSPATH
com.alipay.sofa.jraft.graduationdesign.ServerBootstrap
$1
$2
$3
EOF

JAVA_CONFIG=$(cat $JAVA_CONFIG | xargs echo)

echo $JAVA_CONFIG

JAVA="$JAVA_HOME/bin/java"

HOSTNAME=$(hostname)

$JAVA $JAVA_CONFIG
