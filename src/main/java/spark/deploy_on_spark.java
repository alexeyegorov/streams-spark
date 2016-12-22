package spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import stream.Constants;
import stream.DistributedStream;
import stream.DocumentEncoder;
import stream.SparkStreamTopology;
import stream.runtime.StreamRuntime;
import stream.util.XMLUtils;

/**
 * Build and run Spark topology locally or deploy jar with this class as mainclass to your cluster.
 *
 * @author alexey
 */
public class deploy_on_spark {


    static Logger log = LoggerFactory.getLogger(deploy_on_spark.class);

    /**
     * Method to start cluster and run XML configuration as spark topology on it while setting the
     * maximum running time to Long.MAX_VALUE.
     *
     * @param in path to XML configuration
     */
    public static void main(InputStream in) throws Exception {
        main(in, Long.MAX_VALUE);
    }

    /**
     * Parse XML configuration, create spark topology out of it and run it for some given time.
     *
     * @param in  path to the XML configuration
     * @param time maximum time for a cluster to run
     */
    public static void main(InputStream in, Long time) throws Exception {
        StreamRuntime.loadUserProperties();

//        System.setProperty("rlog.host", "127.0.0.1");
//        System.setProperty("rlog.token", "ab09cfe1d60b602cb7600b5729da939f");

        String xml = storm.run.createIDs(in);

        Document doc = XMLUtils.parseDocument(xml);

        log.info("Encoding document...");
        String enc = DocumentEncoder.encodeDocument(doc);
        log.info("Arg will be:\n{}", enc);

        Document decxml = DocumentEncoder.decodeDocument(enc);
        log.info("Decoded XML is: {}", XMLUtils.toString(decxml));

        SparkConf sparkConf = new SparkConf();

        // set app name
        String appId = Utils.getAppId(doc);
        sparkConf.setAppName(appId);

        Element docElement = doc.getDocumentElement();

        // set master node
        String masterNode = docElement.hasAttribute(Constants.SPARK_MASTER_NODE)
                ? docElement.getAttribute(Constants.SPARK_MASTER_NODE) : "local[2]";
        sparkConf.setMaster(masterNode);

        //FIXME try to use SNAPPY again
        // switch the compression from SNAPPY to LZF due to problems with the snappy library
        sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");

        // set parallelism level
        String parallelism = docElement.hasAttribute("spark.default.parallelism")
                ? docElement.getAttribute("spark.default.parallelism") : "2";
        sparkConf.set("spark.default.parallelism", parallelism);

        sparkConf.set("spark.streaming.backpressure.enabled", "true");

        // set batch interval
        String attribute = docElement.getAttribute(Constants.SPARK_BATCH_INTERVAL);
        long interval;
        try {
            interval = Long.parseLong(attribute);
        } catch (Exception exc) {
            interval = 1000;
        }
        Duration milliseconds = Durations.milliseconds(interval);

        log.info("Creating app '{}' with master node mode '{}' and batch interval of '{}' ms ",
                appId, masterNode, interval);

        SparkStreamTopology sparkStreamTopology = new SparkStreamTopology(doc, sparkConf, milliseconds);

        if (sparkStreamTopology.createTopology()) {
            sparkStreamTopology.executeTopology();
        } else {
            log.info("Do not execute the topology as there were errors while building the topology.");
        }
    }

    /**
     * Entry main method that extracts file path to XML configuration.
     *
     * @param args list of parameters
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Missing file path to XML configuration of streams job to run.");
            return;
        }
        if (args[0].startsWith("hdfs:")) {
            DistributedFileSystem dfs = new DistributedFileSystem();
            dfs.initialize(new URI("hdfs://s876cn01.cs.uni-dortmund.de:8020"), new Configuration());
            Path path = new Path(args[0]);
            if (!dfs.exists(path)) {
                log.error("Path to XML configuration is not valid: {}", path.toString());
                return;
            }
            main(dfs.open(path));
        }else{
            File file = new File(args[0]);
            if (!file.getAbsoluteFile().exists() || !file.exists()) {
                log.error("Path to XML configuration is not valid: {}", file.toString());
                return;
            }
            main(file.toURI().toURL().openStream());
        }
    }
}
