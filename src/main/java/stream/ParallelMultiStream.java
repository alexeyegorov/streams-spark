package stream;

import stream.io.SourceURL;
import stream.io.multi.AbstractMultiStream;

/**
 * Abstract class for parallel multi stream. Each subclass has to implement the method {@link
 * #handleParallelism(int, int)}. E.g. for distributed streams like in Flink or Spark Streaming this
 * method should be called before the serialization and thus we can save serializable settings
 * before the program is distributed over the cluster.
 */
public abstract class ParallelMultiStream extends AbstractMultiStream {

    public ParallelMultiStream(SourceURL url) {
        super(url);
    }

    public ParallelMultiStream() {
        super();
    }

    /**
     * Abstract method that should help handle parallelism especially in a distributed environment.
     *
     * @param instanceNumber number of this instance
     * @param copiesNumber   number of all instantiated instances
     */
    public abstract void handleParallelism(int instanceNumber, int copiesNumber);
}
