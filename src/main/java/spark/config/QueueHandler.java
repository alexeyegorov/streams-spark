package spark.config;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import spark.functions.SparkQueue;
import stream.Data;
import stream.SparkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Configuration handler for queues. Method handle(...) creates SparkQueue which is not a spark
 * native function, but implements Function and Queue classes. It is used as a wrapper for a queue
 * class with extra functionality to add label 'spark.queue' to data items.
 *
 * @author alexey
 */
public class QueueHandler extends SparkConfigHandler {

    static Logger log = LoggerFactory.getLogger(QueueHandler.class);

    private SparkQueue function;

    public QueueHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element el, SparkStreamTopology st) throws Exception {
        if (handles(el)) {
            String id = el.getAttribute("id");
            if (id == null || id.trim().isEmpty()) {
                log.error("No 'id' attribute defined in queue element: {}", el.toString());
                throw new Exception("Missing 'id' attribute for queue element!");
            }

            //TODO handle parallelism?

            function = new SparkQueue(st, el);
        }
    }

    @Override
    public boolean handles(Element el) {
        return handles(el, "queue");
    }

    /**
     * While handling document element some function is created (e.g. FlatMapFunction or Queue).
     *
     * @return function created in the handle(...) method.
     */
    public SparkQueue getFunction() {
        return function;
    }
}
