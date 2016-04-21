package spark.functions;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

import stream.Data;
import stream.io.Stream;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.StreamFactory;
import stream.util.Variables;

/**
 * Own source implementation to embed stream processor from 'streams framework'
 *
 * @author alexey
 */
public class SparkSource extends Receiver<Data> {

    static Logger log = LoggerFactory.getLogger(SparkSource.class);

    /**
     * Stream processor embedded inside of SourceFunction
     */
    protected Stream streamProcessor;

    /**
     * Flag to stop retrieving elements from the source.
     */
    private boolean isRunning = true;

    /**
     * Variables with environment information
     */
    protected Variables variables;

    /**
     * Element object containing part of XML file with configuration for the source.
     */
    private Element el;

    /**
     * Create new spark source object while saving XML's element with source configuration.
     *
     * @param element part of XML with source configuration
     */
    public SparkSource(Variables variables, Element element) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.variables = variables;
        this.el = element;
        log.debug("Source for '" + el + "' initialized.");
    }

    /**
     * init() is called inside of super class' readResolve() method.
     */
    protected void init() throws Exception {
        streamProcessor = StreamFactory.createStream(ObjectFactory.newInstance(), el, variables);
        streamProcessor.init();
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
                    store(data);
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
