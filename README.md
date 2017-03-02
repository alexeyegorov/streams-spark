# Streams Spark [![Build Status](https://travis-ci.org/alexeyegorov/streams-spark.svg?branch=master)](https://travis-ci.org/alexeyegorov/streams-spark)

This project is based upon [streams-flink](https://bitbucket.org/AEgorov/streams-flink) that tries to slightly modify [streams-storm](https://bitbucket.org/cbockermann/streams-storm) project in order to adapt it to ``Flink``.
Here we do the same procedure to adapt it to ``Spark Streaming``.
This way we can achieve parsing of XML configuration files for ``streams framework`` and translating them into ``Spark``'s data flow graph.


The XML definition of ``streams`` process has not been changed.
We have support for ``services`` and ``queues``. 
We can still use ``copies`` attribute in ``<process ...>`` tag in order to controll the level of parallelism.
Each copy is then mapped to a task slot inside of the Flink cluster.
Each ``process``, e.g. as the following
```
<process input="data" id="extraction" copies="1">
```

is translated to a flatMap function.

```
JavaDStream<Data> dataJavaDStream = receiver.flatMap(function);
```
Parallelism level is set over ``copies`` attribute.
In Spark Streaming it can be defined through the number of the partitions in a RDD.
On the one hand it can be set through ``batch interval / block interval`` or we can repartition the data manually as followed:

```
if (el.hasAttribute("copies")) {
	receiver = receiver.repartition(Integer.parseInt(element.getAttribute("copies")));
}
```

This value should be computed as the number of all available cores minus 1 core for the driver.

The easiest way to start a Spark job is to use the submit script by Spark itself (many configurations can be set here or directly in code):

```
./bin/spark-submit --class spark.deploy_on_spark --master spark://129.217.30.197:6066 \
            --deploy-mode cluster \
            --executor-memory 9G \
            --driver-memory 2G \
            --num-executors 2 --executor-cores 5 \
            --conf "spark.eventLog.dir=file:///home/egorov/spark-eventlog" \
            --conf "spark.eventLog.enabled=true" \
            --conf "spark.streaming.unpersist=true" \
            --conf "spark.ui.showConsoleProgress=false" \
            --conf "spark.streaming.backpressure.enabled=true" \
            --conf "spark.streaming.ui.retainedBatches=300" \
            --conf "spark.ui.retainedStages=300" \
            --conf "spark.locality.wait=1s" \
            --conf "spark.locality.wait.node=0s" \
            --conf "spark.locality.wait.rack=1s" \
            --conf "spark.worker.cleanup.enabled=true" \
            --conf "spark.worker.cleanup.interval=1800" \
            --conf "spark.executor.logs.rolling.maxRetainedFiles=4" \
            --conf "spark.executor.logs.rolling.strategy=time" \
            --conf "spark.executor.logs.rolling.time.interval=hourly" \
            --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=2G -XX:+UseConcMarkSweepGC -Dlog4j.configuration=log4j-eir.properties" \
            file:///home/egorov/streams-spark-0.9.25-SNAPSHOT-spark-provided.jar /home/egorov/example.xml
```

``spark.locality.wait{.node,.rack}`` setting can be used to enable splitting the tasks among several nodes.
Otherwise default setting of 3 seconds will be used and possibly all tasks will run on the node where the data is received.

Some further optimizations that are mentioned above has been described [here](https://blog.cloudera.com/blog/2016/01/how-cigna-tuned-its-spark-streaming-app-for-real-time-processing-with-apache-kafka/).
