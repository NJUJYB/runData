#!/bin/bash

cd ~/Desktop/hadoop/hadoop-2.2.0/
./sbin/start-dfs.sh

#ssh slave1 << remotessh
#cd ~/Desktop/hadoop/hadoop-2.2.0/
#./sbin/hadoop-daemon.sh start datanode
#exit
#remotessh

echo Prepare-Hadoop-HDFS
