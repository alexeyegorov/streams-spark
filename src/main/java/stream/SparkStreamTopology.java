package stream;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
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
    private Document doc;
    private JavaStreamingContext jsc;

    /**
     * List of queues used for inter-process communication.
     */
    public List<SparkQueue> sparkQueues = new ArrayList<>(0);

    /**
     * List of services
     */
    public List<SparkService> sparkServices = new ArrayList<>(0);

    public SparkStreamTopology(Document doc, JavaStreamingContext jsc) {
        this.doc = doc;
        this.jsc = jsc;
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

        // handle properties and save them to variables
        variables.addVariables(Utils.handleProperties(doc, variables));

        // handle <service.../>
        initFlinkServices(doc);

        // create stream sources (multiple are possible)
        HashMap<String, JavaReceiverInputDStream<Data>> sources = initSparkSources();
        if (sources == null) {
            log.error("No source was or could have been initialized.");
            return false;
        }

        // create all possible queues
        initSparkQueues(doc);

//        JavaDStream<Long> data = sources.get("data").count();
        // create processor list handler and apply it to ProcessorLists
        return initSparkFunctions(doc, sources);
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
     * Find ProcessorLists and handle them to become FlatMap functions.
     *
     * @param doc     XML document
     * @param sources list of sources / queues
     * @return true if all functions can be applied; false if something goes wrong.
     */
    private boolean initSparkFunctions(Document doc, HashMap<String, JavaReceiverInputDStream<Data>> sources) {
        // check if any function is found to be applied onto data stream
        // spark topology won't stop, if some queue is mentioned but not used and if processor list
        // is using an input queue that is not filled
        boolean anyFunctionFound = false;

        // create processor list handler
        ProcessListHandler handler = new ProcessListHandler(ObjectFactory.newInstance());

        //TODO use getElementsByTagName?
        NodeList list = doc.getDocumentElement().getChildNodes();
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

                        // apply processors
                        Function<Data, Data> function = handler.getFunction();

                        JavaDStream<Data> map = sources.get(input).map(function);
                        map.print();
//                        DataStream<Data> dataStream =
//                                .flatMap(function)
//                                .setParallelism(getParallelism(el));
//
//                        // detect output queues
//                        List<String> outputQueues = function.getListOfOutputQueues();

                        // split the data stream if there are any queues used inside
                        // of process list
//                        if (outputQueues.size() > 0) {
//                            splitDataStream(sources, dataStream, outputQueues);
//                        }
                        anyFunctionFound = true;
                    }
                }
            }
        }
        return anyFunctionFound;
    }

    /**
     * Find all sources (streams) and wrap them in FlinkSources.
     */
    private HashMap<String, JavaReceiverInputDStream<Data>> initSparkSources() {
        NodeList streamList = doc.getDocumentElement().getElementsByTagName("stream");
        if (streamList.getLength() < 1) {
            log.debug("At least 1 stream source has to be defined.");
            return null;
        }

        ObjectFactory of = ObjectFactory.newInstance();
        SourceHandler sourceHandler = new SourceHandler(of);
        HashMap<String, JavaReceiverInputDStream<Data>> sources = new HashMap<>(0);

        //TODO use something more simple?!
        for (int is = 0; is < streamList.getLength(); is++) {
            Element item = (Element) streamList.item(is);
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
                JavaReceiverInputDStream<Data> receiverStream = jsc.receiverStream(sourceHandler.getFunction());

                // put this source into the hashmap
                sources.put(id, receiverStream);
                log.info("'{}' added as stream source.", id);
            } else {
                log.debug("Source handler doesn't handle {}", item.toString());
            }
        }
        return sources;
    }

    /**
     * Find all queues and wrap them in FlinkQueues.
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

//    /**
//     * For a list of processors enqueueing items split the DataStream and put the selected new data
//     * streams into the hashmap.
//     *
//     * @param sources      hash map containing data streams
//     * @param dataStream   data stream used for split
//     * @param outputQueues list of queues used for the output
//     */
//    private static void splitDataStream(HashMap<String, DataStream<Data>> sources,
//                                        DataStream<Data> dataStream,
//                                        List<String> outputQueues) {
//        final List<String> allQueues = outputQueues;
//        SplitStream<Data> split = dataStream.split(new OutputSelector<Data>() {
//            @Override
//            public Iterable<String> select(Data data) {
//                List<String> queues = new ArrayList<>(allQueues.size());
//                try {
//                    if (data.containsKey("spark.queue")) {
//                        String outputQueue = (String) data.get("spark.queue");
//                        log.debug("spark.queue {}", outputQueue);
//                        for (String queue : allQueues) {
//                            if (queue.equals(outputQueue)) {
//                                queues.add(queue);
//                            }
//                        }
//                    }
//                } catch (NullPointerException ex) {
//                    log.error("Data item is empty.");
//                }
//                return queues;
//            }
//        });
//        for (String queue : allQueues) {
//            sources.put(queue, split.select(queue));
//        }
//    }
}

