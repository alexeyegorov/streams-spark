package spark.config;

import org.w3c.dom.Element;

import stream.SparkStreamTopology;
import stream.runtime.setup.factory.ObjectFactory;

/**
 * Abstract class for Spark configuration handlers
 *
 * @author alexey
 */
public abstract class SparkConfigHandler {

    protected final ObjectFactory objectFactory;

    public SparkConfigHandler(ObjectFactory of) {
        this.objectFactory = of;
    }

    /**
     * Handel document element for some special configuration handler.
     *
     * @param el element
     * @param st spark stream topology
     */
    public abstract void handle(Element el, SparkStreamTopology st) throws Exception;

    /**
     * Check if given element can be handler by this configuration handler.
     *
     * @param el element
     * @return true or false
     */
    public abstract boolean handles(Element el);

    /**
     * Unified method for internal usage in configuration handler. Each single handler calls this
     * method with name of element it handles.
     *
     * @param el         document element
     * @param handleable name of the element it can handle
     * @return true, if right element; false otherwise
     */
    protected boolean handles(Element el, String handleable) {
        return handleable.equals(el.getNodeName().toLowerCase());
    }
}
