package spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.scheduler.BatchInfo;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import spark.functions.SparkProcessList;
import stream.Data;

/**
 * Listener for streaming events. SparkProcessorList objects register themselves using this class.
 * After a batch is completed, a finish method is called on each processor list object.
 */
public class BatchFinishListener implements StreamingListener {

    static Logger log = LoggerFactory.getLogger(BatchFinishListener.class);

    /**
     * Array list of registered processors
     */
    private static List<Function<Data, Data>> streamProcessors = new ArrayList<>(0);

    public static void registerProcessor(Function<Data, Data> func) {
        streamProcessors.add(func);
    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted streamingListenerBatchStarted) {
    }

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted streamingListenerBatchCompleted) {
        int ind = streamProcessors.size();
        for (Function<Data, Data> next : streamProcessors) {
            if (SparkProcessList.class.isInstance(next)) {
                try {
                    ((SparkProcessList) next).finish();
                    if (!streamProcessors.remove(next)) {
                        log.debug("Failed to remove stream processor list object after finishing it.");
                    } else {
                        log.debug("Successfully finished and removed stream processor list object.");
                    }
                } catch (Exception e) {
                    log.error("Error while calling finish method on processor list.");
                }
            }
        }
        BatchInfo batchInfo = streamingListenerBatchCompleted.batchInfo();
        System.out.println("Batch completed, Total delay :" + batchInfo.totalDelay().get().toString() + " ms");
        if (batchInfo.numRecords() > 0 || ind > 0) {
            log.info("Batch completed and executed {} times in {} ms with {} number of records",
                    streamProcessors.size(), batchInfo.batchTime().milliseconds(),
                    batchInfo.numRecords());
        }
    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted streamingListenerReceiverStarted) {

    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError streamingListenerReceiverError) {

    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped streamingListenerReceiverStopped) {

    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted streamingListenerBatchSubmitted) {
    }


    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted streamingListenerOutputOperationStarted) {
    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted streamingListenerOutputOperationCompleted) {
    }
}
