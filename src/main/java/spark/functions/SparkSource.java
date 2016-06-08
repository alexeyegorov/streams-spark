package spark.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.ArrayList;

import stream.SparkSourceStream;
import stream.storm.Constants;
import stream.util.Variables;

/**
 * Own source implementation to embed stream processor from 'streams framework'
 *
 * @author alexey
 */
public class SparkSource {

    static Logger log = LoggerFactory.getLogger(SparkSource.class);

    /**
     * Stream processor embedded inside of SourceFunction
     */
    protected ArrayList<SparkSourceStream> streamProcessor;


    /**
     * Variables with environment information
     */
    protected Variables variables;

    /**
     * Create new spark source object while saving XML's element with source configuration.
     *
     * @param variables variables holding all the configuration
     * @param element part of XML with source configuration
     */
    public SparkSource(Variables variables, Element element) {

        this.variables = variables;
        int copies = 1;
        if (element.hasAttribute(Constants.NUM_WORKERS)) {
            String copiesStr = element.getAttribute(Constants.NUM_WORKERS);
            try {
                copies = Integer.parseInt(copiesStr);
            } catch (NumberFormatException exc) {
                copies = 1;
            }
        }
        streamProcessor = new ArrayList<>(copies);
        for (int i = 0; i < copies; i++) {
            streamProcessor.add(new SparkSourceStream(i, copies, element, variables));
        }
        log.debug("Source for '" + element + "' initialized.");
    }

    public ArrayList<SparkSourceStream> getSourceFunctions() {
        return streamProcessor;
    }

}
