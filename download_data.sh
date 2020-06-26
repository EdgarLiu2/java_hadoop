#!/bin/bash
# http://www.youdidwhatwithtsql.com/preparing-ncdc-weather-data-hadoop/1526/

source_url="ftp://ftp.ncdc.noaa.gov/pub/data/noaa/2019";
download_to="/Users/liuzhao/workspace/hadoop/hadoop-3.1.2/test/input";

if [ ! -d "$download_to" ];then
	mkdir -p "$download_to";
fi

wget -r -c --progress=bar --no-parent -P "$download_to" "$source_url";
