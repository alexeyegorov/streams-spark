package spark.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.util.Map;

import stream.runtime.setup.factory.ObjectFactory;
import stream.service.Service;
import stream.util.Variables;

/**
 * SparkService is used as a wrapper for a service class. While building up spark topology, all
 * services are wrapped into SparkServices, initialized after deserialization by Spark and then set
 * by ProcessorCreationHandler to be used by a right processor.
 *
 * @author alexey
 */
public class SparkService extends StreamsSparkObject implements Serializable {

    static Logger log = LoggerFactory.getLogger(SparkService.class);

    /**
     * Variables with environment information
     */
    private final Variables variables;

    /**
     * Document element containing information about service.
     */
    private final Element element;

    /**
     * Service to be used
     */
    private Service service;

    public SparkService(Variables variables, Element element) {
        if (!element.hasAttribute("class") || !element.hasAttribute("id")) {
            log.error("Class or ID attribute are not given for a service.");
            throw new ExceptionInInitializerError("Class or ID attribute are not given for a service.");
        }
        this.variables = variables;
        this.element = element;
    }

    @Override
    protected void init() throws Exception {
        log.debug("Creating new service implementation from class {}", element.getAttribute("class"));
        ObjectFactory obf = ObjectFactory.newInstance();
        obf.addVariables(variables);
        Map<String, String> params = obf.getAttributes(element);
        try {
            service = (Service) obf.create(
                    params.get("class"), params,
                    ObjectFactory.createConfigDocument(element), variables);
            service.reset();

            String input = params.get("input");
            //TODO do we want to handle service references?
//            if (input != null && !input.trim().isEmpty()) {
//                SourceRef sourceRef = new SourceRef(service, "input", input);
//                dependencyInjection.add(sourceRef);
            // this should not be required in the future - handled
            // by dependencyInjection class
            // computeGraph.addReference(sourceRef);
//            }
        } catch (Exception e) {
            log.error("Failed to create and register service '{}': {}",
                    params.get("id"), e.getMessage());
            throw e;
        }
    }

    public Service getService() {
        return service;
    }

    public String getServiceName() {
        return element.getAttribute("id");
    }
}
