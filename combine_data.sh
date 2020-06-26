#!/bin/bash

source=/Users/liuzhao/workspace/hadoop/hadoop-3.1.2/test/input/ncdc/all/2019
target=/Users/liuzhao/workspace/hadoop/hadoop-3.1.2/test/input/ncdc/2019

for file in $source/*
do
	gunzip -c $file >> $target.all
	echo "reporter:status:Processed $file" >&2
done