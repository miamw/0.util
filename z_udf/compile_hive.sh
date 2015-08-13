#/bin/bash

# ./compile_hive.sh ToLowerCase.java tolowercase.jar

rm -r classes
mkdir classes
HADOOP_LIBS=/home/gs/hadoop/current/share/hadoop
libs=`find /home/gs/hadoop/current/share/hadoop | grep jar$`
libs=`echo $libs | sed -e 's/ /:/g'`
echo $libs

export CLASSPATH=$CLASSPATH:$libs:/home/y/libexec/hive/lib/hive-exec.jar

javac -cp $CLASSPATH -d classes $1
jar -cvf $2 -C classes/ .
