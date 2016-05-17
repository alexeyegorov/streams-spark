/*
 *  streams library
 *
 *  Copyright (C) 2011-2014 by Christian Bockermann, Hendrik Blom
 *
 *  streams is a library, API and runtime environment for processing high
 *  volume data streams. It is composed of three submodules "stream-api",
 *  "stream-core" and "stream-runtime".
 *
 *  The streams library (and its submodules) is free software: you can
 *  redistribute it and/or modify it under the terms of the
 *  GNU Affero General Public License as published by the Free Software
 *  Foundation, either version 3 of the License, or (at your option) any
 *  later version.
 *
 *  The stream.ai library (and its submodules) is distributed in the hope
 *  that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import spark.functions.SparkQueue;
import stream.Processor;
import stream.io.Sink;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory.ProcessorCreationHandler;

/**
 * Original QueueInjection used in streams-storm project adapted for use with FlinkQueues
 *
 * @author alexey, christian
 */
public class QueueInjection implements ProcessorCreationHandler {

    static Logger log = LoggerFactory.getLogger(stream.storm.QueueInjection.class);

    /**
     * List of FlinkQueues used as wrapper for real queue implementations.
     */
    private final List<SparkQueue> sparkQueues;

    public QueueInjection(List<SparkQueue> sparkQueues) {
        this.sparkQueues = sparkQueues;
    }

    public static String getQueueSetterName(Method m) {
        return m.getName().substring(3);
    }

    /**
     * @see ProcessorCreationHandler#processorCreated(stream.Processor, Element)
     */
    @Override
    public void processorCreated(Processor p, Element from) throws Exception {
        Map<String, String> params = ObjectFactory.newInstance().getAttributes(from);

        // iterate through all methods to find setter methods for (sub)class of Sink
        for (Method m : p.getClass().getMethods()) {
            log.trace("Checking method {}", m);
            if (DependencyInjection.isSetter(m, Sink.class)) {
                final String qsn = getQueueSetterName(m);

                //TODO: is it necessary or would simply qsn.toLowerCase() would do the same?
                String prop = qsn.substring(0, 1).toLowerCase() + qsn.substring(1);

                if (params.get(prop) == null) {
                    log.debug("Found null-value for property '{}', " +
                            "skipping injection for this property.", prop);
                    continue;
                }

                log.debug("Found queue-setter for property {} (property value: '{}')",
                        prop, params.get(prop));

                if (DependencyInjection.isArraySetter(m, Sink.class)) {
                    // setter using array of comma separated queue names
                    String[] names = params.get(prop).split(",");

                    List<SparkQueue> wrapper = new ArrayList<>();
                    for (String name : names) {
                        if (!name.trim().isEmpty()) {
                            SparkQueue flinkQueue = getSparkQueue(name);
                            if (flinkQueue != null) {
                                wrapper.add(flinkQueue);
                            } else {
                                log.debug("Queue '{}' was not found in the list of defined queues",
                                        name);
                            }
                        }
                    }
                    log.debug("Injecting array of queues...");
                    Object array = wrapper.toArray(new SparkQueue[wrapper.size()]);
                    m.invoke(p, array);

                } else {
                    // setter using queue name
                    String name = params.get(prop);
                    log.debug("Injecting a single queue... using method {}", m);
                    SparkQueue flinkQueue = getSparkQueue(name);
                    if (flinkQueue != null) {
                        m.invoke(p, flinkQueue);
                    } else {
                        log.debug("Queue '{}' was not found in the list of defined queues", name);
                    }
                }
            } else {
                log.debug("Skipping method {} => not a queue-setter", m);
            }
        }
    }

    /**
     * Iterate through the list of all queues and find a queue with the given name
     *
     * @param name queue name
     * @return SparkQueue
     */
    private SparkQueue getSparkQueue(String name) {
        for (SparkQueue queue : sparkQueues) {
            if (queue.getQueueName().equals(name)) {
                return queue;
            }
        }
        return null;
    }
}