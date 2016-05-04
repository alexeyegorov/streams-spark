package spark.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.Map;

import spark.functions.SparkService;
import stream.SparkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Configuration handler for streams' services. Method handle(...) creates SparkService to wrap the
 * underlying service.
 *
 * @author alexey
 */
public class ServiceHandler extends SparkConfigHandler {

    static Logger log = LoggerFactory.getLogger(ServiceHandler.class);

    /**
     * Spark service as a function to hold the concept of functions.
     */
    //TODO find a better way to solve this
    private SparkService function;

    public ServiceHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element element, SparkStreamTopology st) throws Exception {
        log.debug("handling element {}...", element);
        Map<String, String> params = objectFactory.getAttributes(element);

        String className = params.get("class");
        if (className == null || "".equals(className.trim())) {
            throw new Exception("No class name provided in 'class' attribute if Service element!");
        }

        String id = params.get("id");
        if (id == null || "".equals(id.trim())) {
            throw new Exception("No valid 'id' attribute provided for Service element!");
        }

        function = new SparkService(st.getVariables(), element);
    }

    @Override
    public boolean handles(Element el) {
        return handles(el, "service");
    }

    public SparkService getFunction() {
        return function;
    }
}
