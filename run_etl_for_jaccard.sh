#!/bin/bash

JAR_FILE="spark-als-sbt_2.11-0.11.jar"

for level in class line prod na
do
	for soar in 105 104 103 102 101
	do
		for ssn in basic fw ss
		do
			spark-submit --deploy-mode "cluster" --class com.searshc.modeling.HashApp --num-executors 63 --driver-memory 42G --executor-memory 30G --executor-cores 8 \
			${JAR_FILE} etlforjaccard "${level}" "${soar}" "${ssn}"
		done
	done
done
exit 0
