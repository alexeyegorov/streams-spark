package spark.config;

import java.util.ArrayList;

import spark.functions.SparkSourceStream;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * @author alexey
 */
public abstract class SparkSourceConfigHandler extends SparkConfigHandler {

    transient ArrayList<SparkSourceStream> function;

    public SparkSourceConfigHandler(ObjectFactory of) {
        super(of);
    }

    /**
     * While handling document element some function is created (e.g. FlatMapFunction or Queue).
     *
     * @return function created in the handle(...) method.
     */
    public ArrayList<SparkSourceStream> getFunction() {
        return function;
    }
}
