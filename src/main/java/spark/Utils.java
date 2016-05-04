package spark;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * @author alexey
 */
public class Utils {

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
}
