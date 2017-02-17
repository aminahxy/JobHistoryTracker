#! /bin/sh
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
base=`cd "$bin/../"; pwd`
conf="$base/conf"
CLASSPATH="."
for j in `ls $base/*.jar`
do
  CLASSPATH="$CLASSPATH:$j"
done
for j in `ls $base/lib/*.jar`
do
  CLASSPATH="$CLASSPATH:$j"
done
CLASSPATH="$conf:$CLASSPATH"
export CLASSPATH=$CLASSPATH
JAVA_HEAP_MAX="-Xmx5000m"
java -classpath $CLASSPATH $JAVA_HEAP_MAX com.sina.data.historytracker.Manager $1
# java -classpath $CLASSPATH $JAVA_HEAP_MAX test.HiveTester
