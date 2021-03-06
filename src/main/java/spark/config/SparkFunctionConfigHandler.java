package spark.config;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import stream.Data;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * @author alexey
 */
public abstract class SparkFunctionConfigHandler extends SparkConfigHandler {

    transient FlatMapFunction<Data, Data> function;

    public SparkFunctionConfigHandler(ObjectFactory of) {
        super(of);
    }

    /**
     * While handling document element some function is created (e.g. FlatMapFunction or Queue).
     *
     * @return function created in the handle(...) method.
     */
    public FlatMapFunction<Data, Data> getFunction() {
        return function;
    }
}
