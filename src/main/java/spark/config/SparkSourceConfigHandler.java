package spark.config;

import org.apache.spark.streaming.receiver.Receiver;

import stream.runtime.setup.factory.ObjectFactory;

/**
 * @author alexey 
 */
public abstract class SparkSourceConfigHandler extends SparkConfigHandler {

    transient Receiver function;

    public SparkSourceConfigHandler(ObjectFactory of) {
        super(of);
    }

    /**
     * While handling document element some function is created (e.g. FlatMapFunction or Queue).
     *
     * @return function created in the handle(...) method.
     */
    public Receiver getFunction() {
        return function;
    }
}
