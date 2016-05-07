package spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

import stream.runtime.DependencyInjection;
import stream.runtime.setup.handler.PropertiesHandler;
import stream.util.Variables;

/**
 * @author alexey
 */
public class Utils {

    static Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Go recursively through all children of each element and check if they have 'queue' or
     * 'queues' attribute.
     *
     * @param element part of XML configuration containing list of processors.
     * @return list of queues as string
     */
    public static List<String> getOutputQueues(Element element) {
        List<String> output = new ArrayList<>(0);
        NodeList childNodes = element.getChildNodes();
        for (int el = 0; el < childNodes.getLength(); el++) {
            Node item = childNodes.item(el);
            if (item.getNodeType() == Node.ELEMENT_NODE) {
                Element child = (Element) item;
                if (child.hasAttribute("queue")) {
                    output.add(child.getAttribute("queue"));
                }
                if (child.hasAttribute("queues")) {
                    String queues = child.getAttribute("queues");
                    String[] split = queues.split(",");
                    for (String queue : split) {
                        output.add(queue.trim());
                    }
                }
                output.addAll(getOutputQueues(child));
            }
        }
        return output;
    }

    /**
     * Handle properties in XML file and add them to variables.
     *
     * @param doc       XML file as document
     * @param variables map of variables
     */
    public static Variables handleProperties(Document doc, Variables variables) {
        DependencyInjection dependencies = new DependencyInjection();
        try {
            PropertiesHandler handler = new PropertiesHandler();
            handler.handle(null, doc, variables, dependencies);

            if (log.isDebugEnabled()) {
                log.debug("########################################################################");
                log.debug("Found properties: {}", variables);
                for (String key : variables.keySet()) {
                    log.debug("   '{}' = '{}'", key, variables.get(key));
                }
                log.debug("########################################################################");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return variables;
    }
}
