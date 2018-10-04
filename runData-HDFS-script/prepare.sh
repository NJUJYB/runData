#!/bin/bash

cp ~/Desktop/code/runData-HDFS/hadoop-dist/target/hadoop-2.2.0.tar.gz ~/Desktop/hadoop/
cd ~/Desktop/hadoop/
tar -zxvf ~/Desktop/hadoop/hadoop-2.2.0.tar.gz
rm ~/Desktop/hadoop/hadoop-2.2.0.tar.gz
cp ~/Desktop/hadoop/runData-HDFS-script/dis/* ~/Desktop/hadoop/hadoop-2.2.0/etc/hadoop/
cd ~/Desktop/hadoop/hadoop-2.2.0/
mkdir tmp
mkdir hdfs
mkdir hdfs/name
mkdir hdfs/data
bin/hdfs namenode -format
cd ~
scp -r ~/Desktop/hadoop/hadoop-2.2.0 slave1:~/Desktop/hadoop/

echo PREPARE_DONE
