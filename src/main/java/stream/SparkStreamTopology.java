package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import spark.Utils;
import spark.config.ProcessListHandler;
import spark.config.QueueHandler;
import spark.config.ServiceHandler;
import spark.config.SourceHandler;
import spark.functions.SparkQueue;
import spark.functions.SparkService;
import spark.functions.SparkSourceStream;
import stream.runtime.setup.factory.ObjectFactory;
import stream.util.Variables;
import stream.util.XIncluder;

/**
 * Topology builder similar to streams-storm builder.
 *
 * @author alexey
 */
public class SparkStreamTopology {

    static Logger log = LoggerFactory.getLogger(SparkStreamTopology.class);

    public final Variables variables = new Variables();
    private final long batchInterval;
    private Document doc;
    private JavaStreamingContext jsc;
    private SparkConf sparkConf;

    /**
     * List of queues used for inter-process communication.
     */
    public List<SparkQueue> sparkQueues = new ArrayList<>(0);

    /**
     * List of services
     */
    public List<SparkService> sparkServices = new ArrayList<>(0);

    public SparkStreamTopology(Document doc, SparkConf sparkConf, Duration milliseconds) {
        this.doc = doc;
        this.batchInterval = milliseconds.milliseconds();
        this.sparkConf = sparkConf;
        String cores = "2";
        if (sparkConf.contains(Constants.SPARK_EXECUTOR_CORES)) {
            cores = sparkConf.get(Constants.SPARK_EXECUTOR_CORES);
        }
        variables.put(Constants.SPARK_EXECUTOR_CORES, cores);
    }

    public Variables getVariables() {
        return variables;
    }

    /**
     * Creates a new instance of a StreamTopology based on the given document and using stream
     * execution environment
     */
    public boolean createTopology() throws Exception {
        // search for 'application' or 'container' tag and extract its ID
        variables.put(Constants.APPLICATION_ID, Utils.getAppId(doc));

        // handle <include.../>
        doc = new XIncluder().perform(doc, variables);

        // handle properties and save them to variables
        variables.addVariables(Utils.handleProperties(doc, variables));

        // expand all variables
        expandAllVariables(doc);

        // print the expanded Document
        //TODO remove output of XML or add it as a method
        TransformerFactory tf = TransformerFactory.newInstance();
        javax.xml.transform.Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        transformer.transform(new DOMSource(doc),
                new StreamResult(new OutputStreamWriter(System.out, "UTF-8")));

        // detect the partition fraction that depends on the number of partitions and cores
        int numberSources = 1;
        if (variables.containsKey("numberSources")) {
            String sourcesString = variables.get("numberSources");
            try {
                numberSources = Integer.parseInt(sourcesString);
            } catch (NumberFormatException exc) {
                log.error("Error while parsing numberSources number out of {}.", sourcesString);
            }
        }

        int partitionFactor = 1;
        if (variables.containsKey("partitionFactor")) {
            String fractionString = variables.get("partitionFactor");
            try {
                partitionFactor = Integer.parseInt(fractionString);
            } catch (NumberFormatException exc) {
                log.error("Error while parsing partitionFactor number out of {}.", fractionString);
            }
        }

        // number of spark cores available = #sparkCoresMax - #receivers
        int sparkCores = 1;
        if (variables.containsKey("sparkCores")) {
            String coresString = variables.get("sparkCores");
            try {
                sparkCores = Integer.parseInt(coresString);
            } catch (NumberFormatException exc) {
                log.error("Error while parsing sparkCores number out of {}.", coresString);
            }
        }

        // calculate the best block interval that determines the number of partitions
        int blockInterval = (int) (batchInterval * numberSources / (partitionFactor * sparkCores));
        sparkConf.set(
                Constants.SPARK_STREAMING_BLOCK_INTERVAL, String.valueOf(blockInterval));
        log.info("Setting Spark Streaming blockInterval to {}", blockInterval);

        this.jsc = new JavaStreamingContext(sparkConf, new Duration(batchInterval));

        // handle <service.../>
        initFlinkServices(doc);

        // create stream sources (multiple are possible)
        HashMap<String, JavaDStream<Data>> sources = initSparkSources();
        if (sources == null) {
            log.error("No source was or could have been initialized.");
            return false;
        }

        // create all possible queues
        initSparkQueues(doc);

        // create processor list handler and apply it to ProcessorLists
        return initSparkFunctions(doc, sources);
    }

    /**
     * Expand all existing properties.
     */
    public void expandAllVariables(Document doc) {
        expandAllVariables(doc.getChildNodes());
    }

    private void expandAllVariables(NodeList childNodes) {
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node item = childNodes.item(i);
            if (item.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) item;
                NamedNodeMap attributes = element.getAttributes();
                for (int j = 0; j < attributes.getLength(); j++) {
                    Node attr = attributes.item(j);
                    String expand = variables.expand(attr.getNodeValue());
                    attr.setNodeValue(expand);
                }
                if (item.hasChildNodes()) {
                    expandAllVariables(item.getChildNodes());
                }
            }
        }
    }

    /**
     * Execute previously created topology.
     */
    public void executeTopology() throws Exception {
        // execute spark job if we were able to init all the functions
        jsc.start();
        // wait for Spark job zu terminate
        jsc.awaitTermination();
    }

    /**
     * Find all queues and wrap them in FlinkQueues.
     *
     * @param doc XML document
     */
    private void initFlinkServices(Document doc) throws Exception {
        NodeList serviceList = doc.getDocumentElement().getElementsByTagName("service");
        ServiceHandler serviceHandler = new ServiceHandler(ObjectFactory.newInstance());
        for (int iq = 0; iq < serviceList.getLength(); iq++) {
            Element element = (Element) serviceList.item(iq);
            if (serviceHandler.handles(element)) {
                serviceHandler.handle(element, this);
                SparkService sparkService = serviceHandler.getFunction();
                sparkServices.add(sparkService);
            }
        }
    }

    /**
     * Find all sources (streams) and wrap them in SparkSources.
     */
    private HashMap<String, JavaDStream<Data>> initSparkSources() {
        NodeList streamList = doc.getDocumentElement().getElementsByTagName("stream");
        if (streamList.getLength() < 1) {
            log.debug("At least 1 stream source has to be defined.");
            return null;
        }

        ObjectFactory of = ObjectFactory.newInstance();
        SourceHandler sourceHandler = new SourceHandler(of);
        HashMap<String, JavaDStream<Data>> sources = new HashMap<>(0);

        for (int is = 0; is < streamList.getLength(); is++) {
            Element item = (Element) streamList.item(is);
            if (!item.getParentNode().getNodeName().equals("stream")) {
                if (sourceHandler.handles(item)) {
                    // name of the source
                    String id = item.getAttribute("id");

                    // handle the source and create data stream for it
                    try {
                        sourceHandler.handle(item, this);
                    } catch (Exception e) {
                        log.error("Error while handling the source for item {}", item);
                        return null;
                    }

                    ArrayList<SparkSourceStream> functions = sourceHandler.getFunction();

                    // initialize receivers
                    List<JavaDStream<Data>> dataStreams = new ArrayList<>(functions.size());
                    while (functions.size() > 0) {
                        dataStreams.add(jsc.receiverStream(functions.remove(0)));
                    }

                    // unify receivers if there are more than 1
                    JavaDStream<Data> unifiedStream;
                    if (dataStreams.size() > 1) {
                        unifiedStream = jsc.union(dataStreams.get(0),
                                dataStreams.subList(1, dataStreams.size()));
                    } else if (dataStreams.size() == 1) {
                        unifiedStream = dataStreams.remove(0);
                    } else {
                        log.error("NO data stream has been initialized.");
                        return null;
                    }

                    // put this source into the hashmap
                    sources.put(id, unifiedStream);
                    log.info("'{}' added as stream source.", id);
                } else {
                    log.debug("Source handler doesn't handle {}", item.toString());
                }
            } else {
                log.info("Found embedded/multi stream.");
            }
        }
        return sources;
    }

    /**
     * Find ProcessorLists and handle them to become FlatMap functions.
     *
     * @param doc     XML document
     * @param sources list of sources / queues
     * @return true if all functions can be applied; false if something goes wrong.
     */
    private boolean initSparkFunctions(Document doc, HashMap<String, JavaDStream<Data>> sources) {
        // check if any function is found to be applied onto data stream
        // spark topology won't stop, if some queue is mentioned but not used and if processor list
        // is using an input queue that is not filled
        boolean anyFunctionFound = false;

        // create processor list handler
        ObjectFactory objectFactory = ObjectFactory.newInstance();
        objectFactory.addVariables(variables);
        ProcessListHandler handler = new ProcessListHandler(objectFactory);

        NodeList list = doc.getDocumentElement().getElementsByTagName("process");
        int length = list.getLength();
        for (int i = 0; i < length; i++) {
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                final Element el = (Element) node;

                if (handler.handles(el)) {
                    log.info("--------------------------------------------------------------------------------");
                    log.info("Handling element '{}'", node.getNodeName());
                    try {
                        handler.handle(el, this);
                    } catch (Exception e) {
                        log.error("Handler {} could not handle element {}.", handler, el);
                        return false;
                    }
                    String input = el.getAttribute("input");
                    log.info("--------------------------------------------------------------------------------");
                    if (ProcessListHandler.class.isInstance(handler)) {
                        if (!sources.containsKey(input)) {
                            log.error("Input '{}' has not been defined or no other processor is " +
                                    "filling this input queue. Define 'stream' or " +
                                    "put processor list after the processor list defining the " +
                                    "output queue with this input name.", input);
                            continue;
                        }

                        // retrieve the incoming data stream
                        JavaDStream<Data> receiver = sources.get(input);

                        // handle the case of repartitioning (using previous number of
                        // partitions defined through batch and block interval)
                        if (el.hasAttribute(Constants.NUM_WORKERS)) {
                            String copiesStr = el.getAttribute(Constants.NUM_WORKERS);
                            SparkConf conf = jsc.ssc().conf();
                            if (conf.contains(Constants.SPARK_STREAMING_BLOCK_INTERVAL)) {
                                String blockMillis = conf.get(Constants.SPARK_STREAMING_BLOCK_INTERVAL);
                                blockMillis = blockMillis.substring(0, blockMillis.length() - 2);

                                int blockInterval = Integer.parseInt(blockMillis);
                                int copies = Integer.parseInt(copiesStr);

                                // number of partitions used so far
                                double numPartitions = ((double) batchInterval) / blockInterval;

                                // difference between previous number of partitions to the
                                // suggested new number of partitions
                                double partitionDiff = Math.abs((numPartitions - copies) / numPartitions);

                                // repartition DStream if the new level of parallelism is highly
                                // different from the old level
                                if (partitionDiff >= 0.2) {
                                    //TODO use better constant?
                                    receiver = receiver.repartition(copies);
                                }
                            }
                        }

                        // retrieve the processor function
                        final FlatMapFunction<Data, Data> function = handler.getFunction();

                        // apply process functions (processors)
                        JavaDStream<Data> dataJavaDStream = receiver.flatMap(function);

                        //TODO add mapToPair and groupByKey
//                                .mapToPair((PairFunction<Data, String, Data>) data
//                                        -> new Tuple2<>((String) data.get("key"), data))
//                                .groupByKey()

                        //TODO add stateful processing
//                                .updateStateByKey((Function2<List<Data>, Optional< SparkContext>,
//                                        Optional<SparkContext>>) (v1, v2) -> {
//                                    // retrieve context information
//                                    SparkContext sparkContext = v2.get();
//
//                                    // update the state (context)
//                                    ...
//
//                                    return Optional.of(sparkContext);
//                                });

                        // detect output queues
                        List<String> outputQueues = Utils.getOutputQueues(el);

                        // split the data stream if there are any queues used inside
                        // of process list
                        if (outputQueues.size() > 0) {
                            splitDataStream(sources, dataJavaDStream, outputQueues);
                        } else {
                            dataJavaDStream.foreachRDD((VoidFunction<JavaRDD<Data>>) dataJavaRDD -> {
                                long count = dataJavaRDD.count();
                                log.info("Processed {} event items.", count);
                            });
                        }
                        anyFunctionFound = true;
                    }
                }
            }
        }
        return anyFunctionFound;
    }

    /**
     * Find all queues and wrap them in SparkQueues.
     *
     * @param doc XML document
     */
    private void initSparkQueues(Document doc) throws Exception {
        NodeList queueList = doc.getDocumentElement().getElementsByTagName("queue");
        ObjectFactory of = ObjectFactory.newInstance();
        QueueHandler queueHandler = new QueueHandler(of);
        for (int iq = 0; iq < queueList.getLength(); iq++) {
            Element element = (Element) queueList.item(iq);
            if (queueHandler.handles(element)) {
                queueHandler.handle(element, this);
                SparkQueue sparkQueue = queueHandler.getFunction();
                sparkQueues.add(sparkQueue);
            }
        }
    }

    /**
     * For a list of processors enqueueing items split the DataStream and put the selected new data
     * streams into the hashmap.
     *
     * @param sources      hash map containing data streams
     * @param dataStream   data stream used for split
     * @param outputQueues list of queues used for the output
     */
    private static void splitDataStream(HashMap<String, JavaDStream<Data>> sources,
                                        JavaDStream<Data> dataStream,
                                        List<String> outputQueues) {
        for (final String queue : outputQueues) {
            JavaDStream<Data> filtered = dataStream.filter((Function<Data, Boolean>) data -> {
                if (data.containsKey(Constants.SPARK_QUEUE)) {
                    String outputQueue = (String) data.get(Constants.SPARK_QUEUE);
                    log.debug("spark.queue {}", outputQueue);
                    if (queue.equals(outputQueue)) {
                        return true;
                    }
                }
                return false;
            });
            sources.put(queue, filtered);
        }
    }
}

