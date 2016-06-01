#!/usr/bin/env bash


if [ $# != 1 ]
	then
	echo "An argument is required: start or stop."
	exit -1
fi

username="egorov"
spark_master="ls8ws007"
spark_worker1="ls8ws008"
spark_worker2="ls8ws009"
filename=$2

printf "\n\nUser: $username\nMaster: $spark_master\nWorker1: $spark_worker1\nWorker2: $spark_worker2\n\n"


if [[ $1 == "start" ]]; then

    # start Spark master
    echo "Start master..."
    master_start_cmd="cd spark-master; ./sbin/start-master.sh;"
    start_master=`ssh "$username"@"$spark_master".cs.uni-dortmund.de "$master_start_cmd"`

    workerstartcmd="./sbin/start-slave.sh spark://$spark_master.cs.uni-dortmund.de:7077;"

    # start Spark worker 1
    echo "Start worker 1..."
    slaveone_start_cmd="cd spark-worker1; $workerstartcmd"
    start_master=`ssh "$username"@"$spark_worker1".cs.uni-dortmund.de "$slaveone_start_cmd"`

    # start Spark worker 2
    echo "Start worker 2..."
    slavetwo_start_cmd="cd spark-worker2; $workerstartcmd"
    start_master=`ssh "$username"@"$spark_worker2".cs.uni-dortmund.de "$slaveone_start_cmd"`
else

    stopcmd="./sbin/stop-all.sh;"

    # stop Spark master
    echo "Stop master..."
    master_start_cmd="cd spark-master; $stopcmd"
    start_master=`ssh "$username"@"$spark_master".cs.uni-dortmund.de "$master_start_cmd"`


    workerstopcmd="$stopcmd cd work; pwd; rm -rf app-*; rm -rf driver-*;"

    # stop Spark worker 1
    echo "Stop worker 1..."
    slaveone_start_cmd="cd spark-worker1; $workerstopcmd"
    start_master=`ssh "$username"@"$spark_worker1".cs.uni-dortmund.de "$slaveone_start_cmd"`

    # stop Spark worker 2
    echo "Stop worker 2..."
    slavetwo_start_cmd="cd spark-worker2; $workerstopcmd"
    start_master=`ssh "$username"@"$spark_worker2".cs.uni-dortmund.de "$slaveone_start_cmd"`
fi

