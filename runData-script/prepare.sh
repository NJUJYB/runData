#!/bin/bash

cp ~/Desktop/code/runData/spark-1.3.1-bin-2.2.0.tgz ~/Desktop/spark/
cd ~/Desktop/spark/
tar -zxvf ./spark-1.3.1-bin-2.2.0.tgz
rm -rf ./spark-1.3.1-bin-2.2.0.tgz
mv spark-1.3.1-bin-2.2.0 runData
cp ~/Desktop/spark/runData-script/* ~/Desktop/spark/runData/conf/
scp -r ~/Desktop/spark/runData/ jyb@slave1:~/Desktop/spark/
rm ~/Desktop/spark-examples_2.10-1.3.1.jar
cp ~/Desktop/code/runData/examples/target/spark-examples_2.10-1.3.1.jar ~/Desktop/

echo "1000" > ~/Desktop/spark/runData/bandwidth.txt
ssh slave1 << remotessh
echo "800" > ~/Desktop/spark/runData/bandwidth.txt
exit
remotessh

echo PREPARE_DONE
