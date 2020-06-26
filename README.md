```bash
cd ~/workspace/hadoop/hadoop-3.1.2
bin/hdfs namenode -format
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/liuzhao
bin/hdfs dfs -mkdir /user/liuzhao/input
bin/hdfs dfs -put etc/hadoop/*.xml /user/liuzhao/input
bin/hdfs dfs -put file.gz /user/liuzhao/input
bin/hadoop fs -ls /user/liuzhao/input/

sbin/start-dfs.sh
sbin/start-yarn.sh
sbin/stop-dfs.sh
sbin/stop-yarn.sh

export JAVA_HOME=export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.1.jdk/Contents/Home
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
bin/hadoop com.sun.tools.javac.Main test/WordCount.java
cd test && jar cf wc.jar WordCount*.class && cd ..
bin/hadoop fs -ls /user/liuzhao/input/
bin/hadoop fs -rm -r -skipTrash /user/liuzhao/output
bin/hadoop jar test/wc.jar WordCount /user/liuzhao/input /user/liuzhao/output
bin/hadoop fs -ls /user/liuzhao/output/
bin/hadoop fs -cat /user/liuzhao/output/part-r-00000

mvn package -Dmaven.test.skip=true
```
