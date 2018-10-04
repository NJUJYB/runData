#!/bin/bash
cd ~
rm -rf ~/Desktop/spark/runData/
ssh slave1 << remotessh
rm -rf ~/Desktop/spark/runData/
exit
remotessh

echo ALL-CLEAN-DONE
