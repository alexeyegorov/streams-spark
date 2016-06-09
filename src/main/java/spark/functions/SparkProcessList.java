package spark.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import spark.BatchFinishListener;
import spark.QueueInjection;
import spark.ServiceInjection;
import spark.Utils;
import stream.Constants;
import stream.Data;
import stream.ProcessContext;
import stream.Processor;
import stream.ProcessorList;
import stream.SparkStreamTopology;
import stream.StatefulProcessor;
import stream.runtime.setup.factory.ObjectFactory;
import stream.runtime.setup.factory.ProcessorFactory;
import stream.util.Variables;

/**
 * Own implementation of FlatMapFunction for a list of processors (<process>...</process>). FlatMap
 * required to be sure all items stored in queues are collected.
 *
 * @author alexey
 */
public class SparkProcessList extends StreamsSparkObject implements FlatMapFunction<Data, Data> {

    static Logger log = LoggerFactory.getLogger(SparkProcessList.class);

    /**
     * List of queues
     */
    private List<SparkQueue> sparkQueues;

    /**
     * List of services
     */
    private List<SparkService> sparkServices;

    /**
     * List of processors to be executed
     */
    protected ProcessorList process;

    /**
     * Variables with environment information
     */
    protected Variables variables;

    /**
     * Document element containing information about list of processors.
     */
    protected Element element;

    /**
     * Process context is used for initialization and is realized here by using SparkContext.
     */
    protected ProcessContext context;

    public SparkProcessList(SparkStreamTopology streamTopology, Element el) {
        super();
        this.variables = streamTopology.getVariables();
        this.element = el;
        String processId;
        if (el.hasAttribute("id")) {
            processId = el.getAttribute("id");
        } else {
            processId = UUID.randomUUID().toString();
        }
        this.context = new SparkContext(processId);
        this.context.set(Constants.APPLICATION_ID,
                streamTopology.variables.get(Constants.APPLICATION_ID));

        // add only queues that are used in this ProcessorList
        List<String> listOfOutputQueues = Utils.getOutputQueues(this.element);
        sparkQueues = new ArrayList<>(0);
        for (SparkQueue queue : streamTopology.sparkQueues) {
            if (listOfOutputQueues.contains(queue.getQueueName())) {
                sparkQueues.add(queue);
            }
        }

        // add services
        this.sparkServices = streamTopology.sparkServices;

        // create process to initialize processors in the list, but do not save it
        // as it will be reinitialized after deserialization process
        try {
            createProcess();
        } catch (Exception e) {
            log.error("Error while creating process\n" + e.getMessage());
            e.printStackTrace();
            //TODO do we have a nicer way to stop the program?
            System.exit(-1);
        }

        log.debug("Processors for '" + el + "' initialized.");
    }

    @Override
    protected void init() throws Exception {
//        int numberOfCores = 1;
//        if (variables.containsKey(Constants.SPARK_EXECUTOR_CORES)) {
//            String s = variables.get(Constants.SPARK_EXECUTOR_CORES);
//            numberOfCores = Integer.parseInt(s);
//        }
        //TODO handle number of cores for logging performance right
        BatchFinishListener.setNumberOfCores(1);
        BatchFinishListener instance = BatchFinishListener.getInstance();
        log.info("Initializing ProcessorList with BatchListener {}", instance);

        // add process identifier using localhost name and some random unique identifier
        String id = element.getAttribute("id") + "@"
                + InetAddress.getLocalHost().getHostName().hashCode()
                + "::" + BatchFinishListener.getThreadNumber();
        element.setAttribute("id", id);
        context.set("process", id);

        process = createProcess();
        for (Processor p : process.getProcessors()) {
            if (p instanceof StatefulProcessor) {
                ((StatefulProcessor) p).init(context);
            }
        }

        // register this object to BatchFinishListener that calls finish at the end of the batch
        instance.registerProcessor(this);
    }

    /**
     * This method creates the inner processors of this process bolt.
     *
     * @return list of processors inside a function
     */
    protected ProcessorList createProcess() throws Exception {
        ProcessorList process;
        ObjectFactory obf = ObjectFactory.newInstance();
        obf.addVariables(variables);
        ProcessorFactory pf = new ProcessorFactory(obf);

        // The handler injects wrappers for any QueueService accesses, thus
        // effectively doing the queue-flow injection
        //
        QueueInjection queueInjection = new QueueInjection(sparkQueues);
        pf.addCreationHandler(queueInjection);

        ServiceInjection serviceInjection = new ServiceInjection(sparkServices);
        pf.addCreationHandler(serviceInjection);

        // create nested processors (list of processors) to be executed
        log.debug("Creating processor-list from element {}", element);
        List<Processor> list = pf.createNestedProcessors(element);
        process = new ProcessorList();
        for (Processor p : list) {
            process.getProcessors().add(p);
        }
        return process;
    }

    @Override
    public Iterable<Data> call(Data data) throws Exception {
        ArrayList<Data> iterable = new ArrayList<>(0);
        if (data != null) {
            process.process(data);

            //TODO add items collected through queues
            // go through all queues and collect written data items
            for (SparkQueue q : sparkQueues) {
                while (q.getSize() > 0) {
                    iterable.add(q.read());
                }
            }
        }

        return iterable;
    }

    /**
     * Perform finish method on internal processor list object.
     */
    public void finish() throws Exception {
        if (process != null) {
            process.finish();
            log.info("Call finish on processor list.");
        }
    }
}
