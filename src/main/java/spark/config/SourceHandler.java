package spark.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.Map;

import spark.functions.SparkSource;
import stream.Constants;
import spark.functions.SparkSourceStream;
import stream.SparkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Configuration handler for streams sources. Method handle(...) creates SourceFunction to produce
 * stream of data.
 *
 * @author alexey
 */
public class SourceHandler extends SparkSourceConfigHandler {

    static Logger log = LoggerFactory.getLogger(SourceHandler.class);

    protected ArrayList<SparkSourceStream> function;

    public SourceHandler(ObjectFactory of) {
        super(of);
    }

    @Override
    public void handle(Element el, SparkStreamTopology st)
            throws Exception {
        if (!handles(el)) {
            return;
        }

        String id = el.getAttribute(Constants.ID);
        if (id == null) {
            throw new Exception("Element '" + el.getNodeName() + "' is missing an 'id' attribute!");
        }

        String className = el.getAttribute("class");
        Map<String, String> params = objectFactory.getAttributes(el);

        log.info("  > Found '{}' definition, with class: {}", el.getNodeName(), className);
        log.info("  >   Parameters are: {}", params);

        params = st.getVariables().expandAll(params);
        log.info("  >   Expanded parameters: {}", params);

        log.info("  >   Creating spout-instance from class {}, parameters: {}", className, params);
        function = new SparkSource(st.getVariables(), el).getSourceFunctions();
    }


    @Override
    public boolean handles(Element el) {
        return handles(el, "stream");
    }

    @Override
    public ArrayList<SparkSourceStream> getFunction() {
        return function;
    }
}
