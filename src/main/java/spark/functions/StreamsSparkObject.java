package spark.functions;

/**
 * Abstract class with an implemented readResolve() and abstract init() methods. This is needed
 * as Spark serializes everything before sending the packaged to the  in order to support
 * serialization inside of Spark.
 *
 * @author alexey
 */
public abstract class StreamsSparkObject {

    /**
     * Init method has to be implemented in order to provide right behaviour after deserialization.
     */
    protected abstract void init() throws Exception;

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
}
