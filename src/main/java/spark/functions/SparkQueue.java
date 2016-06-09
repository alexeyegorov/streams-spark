package spark.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.util.Collection;

import stream.Constants;
import stream.Data;
import stream.SparkStreamTopology;
import stream.io.Queue;
import stream.runtime.setup.factory.ObjectFactory;
import stream.util.Variables;

/**
 * While SparkQueue implements Function and Queue classes, it is used as a wrapper for a queue class
 * with extra functionality to add label 'spark.queue' to data items.
 *
 * @author alexey
 */
public class SparkQueue extends StreamsSparkObject implements Serializable, Queue {

    static Logger log = LoggerFactory.getLogger(SparkQueue.class);

    /**
     * Document element containing information about the queue
     */
    private Element element;

    /**
     * Name of the queue
     */
    private String id;

    /**
     * Variables with environment information
     */
    private Variables variables;

    /**
     * Real queue implementation
     */
    private Queue queue;

    public SparkQueue(SparkStreamTopology streamTopology, Element element) {
        this.element = element;
        variables = streamTopology.getVariables();
        queue = null;
        id = element.getAttribute("id");
    }

    /**
     * Retrieve the name of the queue
     *
     * @return queue name
     */
    public String getQueueName() {
        return id;
    }

    @Override
    public void init() throws Exception {
        ObjectFactory obf = ObjectFactory.newInstance();
        obf.addVariables(variables);
        String className;
        if (!element.hasAttribute("class")) {
            className = "stream.io.BlockingQueue";
        } else {
            className = element.getAttribute("class");
        }
        try {
            this.queue = (Queue) obf.create(className, obf.getAttributes(element), element, variables);
        } catch (ClassCastException ex) {
            log.debug("Queue seems not to be a right queue: {} with class {}",
                    element, obf.findClassForElement(element));
        }
    }

    @Override
    public String getId() {
        return queue.getId();
    }

    @Override
    public void setId(String id) {
        queue.setId(id);
    }

    @Override
    public Data read() throws Exception {
        return queue.read();
    }

    @Override
    public boolean write(Data item) throws Exception {
        item.put(Constants.SPARK_QUEUE, id);
        return queue.write(item);
    }

    @Override
    public boolean write(Collection<Data> data) throws Exception {
        for (Data item : data) {
            item.put(Constants.SPARK_QUEUE, id);
        }
        return queue.write(data);
    }

    @Override
    public void close() throws Exception {
        queue.close();
    }

    @Override
    public void setCapacity(Integer limit) {
        queue.setCapacity(limit);
    }

    @Override
    public Integer getSize() {
        return queue.getSize();
    }

    @Override
    public Integer getCapacity() {
        return queue.getCapacity();
    }

    @Override
    public int clear() {
        return queue.clear();
    }
}
