#!/usr/bin/env bash

#echo "------------------"
#echo "Rebuild the jar..."
#echo "------------------"
#mvn -P deploy package -DskipTests

echo "------------------"
echo "Copy filed to the server..."
echo "------------------"
echo "1. examples/example.xml"
scp examples/example.xml egorov@ls8ws007.cs.uni-dortmund.de:/home/egorov/
#echo "2. target/streams-spark-0.9.26-SNAPSHOT-spark-provided.jar"
#scp target/streams-spark-0.9.26-SNAPSHOT-spark-provided.jar egorov@ls8ws007.cs.uni-dortmund.de:/home/egorov/

echo "------------------"
echo "Submit the job..."
echo "------------------"
cd /Users/alexey/Downloads/spark-1.6.1-bin-hadoop2.3/
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
            --conf "spark.streaming.receiver.maxRate=38" \
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
            file:///home/egorov/streams-spark-0.9.26-SNAPSHOT-spark-provided.jar /home/egorov/example.xml

#            --conf "spark.rdd.compress=true" \
#            --conf "spark.locality.wait.process=5s" \
#            --conf "spark.shuffle.consolidateFiles=true" \
#            --conf "spark.kryoserializer.buffer.mb=24" \
#            --conf "spark.kryoserializer.buffer.max.mb=128" \
#            --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
#            --conf "spark.streaming.blockInterval=1500ms" \
#            --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=7077 \
#            --conf "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=n,address=vpn31135.itmc.tu-dortmund.de:7077,suspend=y,onthrow=<FQ exception class name>,onuncaught=<y/n>" \
