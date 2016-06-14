package stream;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import scala.collection.mutable.ArrayBuffer;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.StreamFactory;
import stream.util.Variables;

public class SparkSourceStream extends Receiver<Data> {

    static Logger log = LoggerFactory.getLogger(SparkSourceStream.class);

    private final Variables variables;

    private final Element el;

    /**
     * Flag to stop retrieving elements from the source.
     */
    private boolean isRunning = true;

    private ParallelSparkMultiStream streamProcessor;

    private int instanceNumber;
    private int copiesNumber;

    public SparkSourceStream(int instanceNumber, int copiesNumber, Element el, Variables variables) {
        //TODO decide what storage level should be used
        super(StorageLevel.MEMORY_AND_DISK_SER());
        this.instanceNumber = instanceNumber;
        this.copiesNumber = copiesNumber;
        this.el = el;
        this.variables = variables;
        log.info("Create SparkSourceStream. Instance {} out of {}.", instanceNumber, copiesNumber);

        try {
            streamProcessor = (ParallelSparkMultiStream) StreamFactory.createStream(ObjectFactory.newInstance(), el, variables);
            streamProcessor.getClass().getMethod("handleParallelism", int.class, int.class);
            streamProcessor.handleParallelism(instanceNumber, copiesNumber);

            // reset to null to enable serialization
            streamProcessor = null;
        } catch (Exception e) {
            log.error("Initializing spark source stream failed during creation phase.");
        }
    }

    /**
     * init() is called inside of super class' readResolve() method.
     */
    protected void init() throws Exception {
        streamProcessor = (ParallelSparkMultiStream) StreamFactory.createStream(ObjectFactory.newInstance(), el, variables);
        try{
            Class<?> aClass = streamProcessor.getClass();
            aClass.getMethod("handleParallelism", int.class, int.class);
            streamProcessor.handleParallelism(instanceNumber, copiesNumber);
        }catch(NoSuchMethodException exc){
            log.info("Stream is not prepared to be handled in parallel.");
        }
        streamProcessor.init();
    }

    /**
     * readResolve() is called every time an object has been deserialized. Inside of it init()
     * method is called in order to provide right behaviour after deserialization.
     *
     * @return this object
     */
    public Object readResolve() throws Exception {
        init();
        return this;
    }

    @Override
    public void onStart() {
        new Thread() {
            @Override
            public void run() {
                streamDataItems();
            }
        }.start();
    }

    /**
     * Execute stream processor and store data items in Spark.
     */
    private void streamDataItems() {
        if (streamProcessor == null) {
            log.debug("Stream processor has not been initialized properly.");
            return;
        }
        isRunning = true;
        while (isRunning && !isStopped()) {
            // Stream processor retrieves next element by calling readNext() method
            // stop if stream is finished and produces NULL
            try {
                Data data = streamProcessor.read();
                if (data != null) {
                    ArrayList<Data> list = new ArrayList<>();
                    list.add(data);
                    store(list.iterator());
//                    store(data);
                } else {
                    isRunning = false;
                }
            } catch (IOException exc) {
                if (exc.getMessage().trim().toLowerCase().equals("stream closed")) {
                    isRunning = false;
                }
            } catch (Exception e) {
                log.error("Error while reading next data item: \n" + e.getMessage());
                isRunning = false;
            }
        }
    }

    @Override
    public void onStop() {
        log.debug("Cancelling SparkSource '" + el + "'.");
        isRunning = false;
    }
}