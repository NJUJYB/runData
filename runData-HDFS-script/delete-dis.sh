#!/bin/bash
cd ~
rm -rf ~/Desktop/hadoop/hadoop-2.2.0/
ssh slave1 << remotessh
rm -rf ~/Desktop/hadoop/hadoop-2.2.0/
exit
remotessh

echo ALL-CLEAN-DONE
