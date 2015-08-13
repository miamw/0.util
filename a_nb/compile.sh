#/bin/bash
rm -r classes
mkdir classes
HADOOP_LIBS=/home/gs/hadoop/current/share/hadoop
libs=`find /home/gs/hadoop/current/share/hadoop | grep jar$`
libs=`echo $libs | sed -e 's/ /:/g'`
echo $libs

export CLASSPATH=$CLASSPATH:$libs

javac -cp $CLASSPATH -d classes ./src/*.java 
rm hadoop_bayes.jar
jar -cvf hadoop_bayes.jar -C classes/ .
