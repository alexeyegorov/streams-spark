package stream;

import stream.io.SourceURL;
import stream.io.multi.AbstractMultiStream;

/**
 * Created by alexey on 02.06.16.
 */
public abstract class ParallelSparkMultiStream extends AbstractMultiStream {

    public ParallelSparkMultiStream(SourceURL url) {
        super(url);
    }

    public ParallelSparkMultiStream() {
        super();
    }

    public abstract void handleParallelism(int instanceNumber, int copiesNumber);
}
