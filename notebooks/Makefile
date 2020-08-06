
HADOOP_VERSION=2.7.6
HADOOP_HOME=/export/hadoop-${HADOOP_VERSION}
HADOOP_TOOLS=${HADOOP_HOME}/share/hadoop/tools/lib
HDFS_DIR=/user/${USER}
 
SAMPLES = sample01.txt sample02.txt sample03.txt sample04.txt

copy_to_hdfs: ${SAMPLES}
	hdfs dfs -mkdir -p ${HDFS_DIR}/input
	hdfs dfs -put $^ ${HDFS_DIR}/input

run_with_hadoop: 
	hadoop jar ${HADOOP_TOOLS}/hadoop-streaming-${HADOOP_VERSION}.jar \
    -file  ${PWD}/mapper.py  -mapper  ${PWD}/mapper.py \
    -file  ${PWD}/reducer.py -reducer ${PWD}/reducer.py \
    -input ${HDFS_DIR}/input/*.txt -output ${HDFS_DIR}/output-hadoop

run_with_yarn: 
	yarn jar ${HADOOP_TOOLS}/hadoop-streaming-${HADOOP_VERSION}.jar \
	-file  ${PWD}/mapper.py  -mapper  ${PWD}/mapper.py \
	-file  ${PWD}/reducer.py -reducer ${PWD}/reducer.py \
	-input ${HDFS_DIR}/input/*.txt -output ${HDFS_DIR}/output-yarn
