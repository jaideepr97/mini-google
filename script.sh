#!/bin/bash

echo enter absolute path of iduRLpairs.txt on your machine	
read idurlpath

echo enter absolute path of files on your machine, omit final slash
read localpath



echo enter absolute path on your hdfs
read hdfspath


hdfs dfs -put $localpath/* $hdfspath

hdfs dfs -put $idurlpath $hdfspath 

