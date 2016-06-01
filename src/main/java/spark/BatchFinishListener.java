package spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import spark.functions.SparkProcessList;
import stream.Data;

/**
 * Listener for streaming events. SparkProcessorList objects register themselves using this class.
 * After a batch is completed, a finish method is called on each processor list object.
 */
public class BatchFinishListener {

    static Logger log = LoggerFactory.getLogger(BatchFinishListener.class);

    static private int THREAD = 0;

    /**
     * Array list of registered processors
     */

    private static class TimedFunction {
        Time time;
        FlatMapFunction<Data, Data> func;

        TimedFunction(Time time, FlatMapFunction<Data, Data> func) {
            this.time = time;
            this.func = func;
        }
    }

    private static List<TimedFunction> timedBatchProcessors = new ArrayList<>(0);

    private static int numberOfCores = 1;

    public static void setNumberOfCores(int number) {
        numberOfCores = number;
    }

    private static int counter = 0;

    private static BatchFinishListener instance;

    public static BatchFinishListener getInstance() {
        if (BatchFinishListener.instance == null) {
            BatchFinishListener.instance = new BatchFinishListener(numberOfCores);
            counter = 0;
        }
        return BatchFinishListener.instance;
    }

    public BatchFinishListener(int numberOfCores) {
        BatchFinishListener.numberOfCores = numberOfCores;
        log.info("Create BatchFinishListener object.");

        TimerTask action = new TimerTask() {
            public void run() {
                BatchFinishListener.getInstance()
                        .finishProcessors(new Time(System.currentTimeMillis()));
                log.info("Scheduled Task.");
            }
        };

        new Timer().schedule(action, 1000, 5000);
    }

    private static final Object sync = new Object();

    public void registerProcessor(FlatMapFunction<Data, Data> func) {
        Time latestBatch;
        synchronized (sync) {
            latestBatch = new Time(System.currentTimeMillis());
            timedBatchProcessors.add(new TimedFunction(latestBatch, func));
            log.info("Register processor {} function", counter++);
        }
        THREAD = (THREAD + 1) % numberOfCores;

//        finishProcessors(latestBatch);
    }

    public static int getThreadNumber() {
        return THREAD;
    }

    public void finishProcessors(final Time time) {
        synchronized (sync) {
            for (int i = timedBatchProcessors.size(); i > 0; i--) {
                int j = i - 1;
                TimedFunction timedFunction = timedBatchProcessors.get(j);
                if (timedFunction.time.less(time.$minus(new Duration(5000)))) {
                    final FlatMapFunction<Data, Data> func = timedFunction.func;
                    if (SparkProcessList.class.isInstance(func)) {
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    ((SparkProcessList) func).finish();
                                } catch (Exception e) {
                                    log.error("Error while calling finish method on processor list.");
                                }
                            }

                        }).start();
                        timedBatchProcessors.remove(j);
                        log.info("Successfully finished and removed stream processor list object with {} processors.", timedBatchProcessors.size());
                    }
                }
            }
        }
    }
}
