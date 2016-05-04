/*
 *  streams library
 *
 *  Copyright (C) 2011-2014 by Christian Bockermann, Hendrik Blom
 *
 *  streams is a library, API and runtime environment for processing high
 *  volume data streams. It is composed of three submodules "stream-api",
 *  "stream-core" and "stream-runtime".
 *
 *  The streams library (and its submodules) is free software: you can
 *  redistribute it and/or modify it under the terms of the
 *  GNU Affero General Public License as published by the Free Software
 *  Foundation, either version 3 of the License, or (at your option) any
 *  later version.
 *
 *  The stream.ai library (and its submodules) is distributed in the hope
 *  that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import stream.runtime.DependencyInjection;
import stream.runtime.setup.handler.PropertiesHandler;
import stream.util.Variables;

/**
 * @author Christian Bockermann &lt;christian.bockermann@udo.edu&gt;
 */
public class StreamTopology {

    static Logger log = LoggerFactory.getLogger(StreamTopology.class);

//    public final TopologyBuilder builder;
//    public final Map<String, BoltDeclarer> bolts = new LinkedHashMap<>();
//    public final Map<String, SpoutDeclarer> spouts = new LinkedHashMap<>();
//    public final Variables variables = new Variables();


//    public Variables getVariables() {
//        return variables;
//    }
//
//    final Set<Subscription> subscriptions = new LinkedHashSet<>();
//
//    /**
//     * Create StreamTopology without topology builder (for spark purpose)
//     */
//    private StreamTopology() {
//        this.builder = null;
//    }

//    /**
//     * @param builder topology builder provided by Apache Storm
//     */
//    private StreamTopology(TopologyBuilder builder) {
//        this.builder = builder;
//    }
//
//    public void addSubscription(Subscription sub) {
//        subscriptions.add(sub);
//    }
//
//    /**
//     * Creates a new instance of a StreamTopology based on the given document and using the
//     * specified TopologyBuilder.
//     */
//    public static StreamTopology build(Document doc, TopologyBuilder builder) throws Exception {
//
//        final StreamTopology st = new StreamTopology(builder);
//
//        doc = XMLUtils.addUUIDAttributes(doc, Constants.UUID_ATTRIBUTE);
//        String appId = "application:" + UUID.randomUUID().toString();
//        NodeList nodeList = doc.getElementsByTagName("application");
//        if (nodeList.getLength() < 1) {
//            nodeList = doc.getElementsByTagName("container");
//        }
//        if (nodeList.getLength() > 1) {
//            log.error("More than 1 application node.");
//        } else {
//            appId = nodeList.item(0).getAttributes().getNamedItem("id").getNodeValue();
//        }
//        st.getVariables().put("application.id", appId);
//        String xml = XMLUtils.toString(doc);
//
//        // a map of pre-defined inputs, i.e. input-names => uuids
//        // to catch the case when processes read from queues that have
//        // not been explicitly defined (i.e. 'linking bolts')
//        //
//        // Map<String, String> streams = new LinkedHashMap<String, String>();
//        ObjectFactory of = ObjectFactory.newInstance();
//
//        st.getVariables().addVariables(handleProperties(doc, st.getVariables()));
//
//        List<ConfigHandler> handlers = new ArrayList<>();
//        handlers.add(new SpoutHandler(of));
//        handlers.add(new stream.storm.config.QueueHandler(of));
//        handlers.add(new StreamHandler(of));
//        handlers.add(new BoltHandler(of));
//        handlers.add(new ProcessHandler(of));
//
//        NodeList list = doc.getDocumentElement().getChildNodes();
//        int length = list.getLength();
//
//        for (ConfigHandler handler : handlers) {
//
//            for (int i = 0; i < length; i++) {
//                Node node = list.item(i);
//                if (node.getNodeType() == Node.ELEMENT_NODE) {
//                    Element el = (Element) node;
//
//                    if (handler.handles(el)) {
//                        log.info("--------------------------------------------------------------------------------");
//                        log.info("Handling element '{}'", node.getNodeName());
//                        handler.handle(el, st, builder);
//                        log.info("--------------------------------------------------------------------------------");
//                    }
//                }
//            }
//        }
//
//        //
//        // resolve subscriptions
//        //
//        Iterator<Subscription> it = st.subscriptions.iterator();
//        log.info("--------------------------------------------------------------------------------");
//        while (it.hasNext()) {
//            Subscription s = it.next();
//            log.info("   {}", s);
//        }
//        log.info("--------------------------------------------------------------------------------");
//        it = st.subscriptions.iterator();
//
//        while (it.hasNext()) {
//            Subscription subscription = it.next();
//            log.info("Resolving subscription {}", subscription);
//
//            BoltDeclarer subscriber = st.bolts.get(subscription.subscriber());
//            if (subscriber != null) {
//                String source = subscription.source();
//                String stream = subscription.stream();
//                if (stream.equals("default")) {
//                    log.info("connecting '{}' to shuffle-group '{}'", subscription.subscriber(), source);
//                    subscriber.shuffleGrouping(source);
//                } else {
//                    log.info("connecting '{}' to shuffle-group '{}:" + stream + "'", subscription.subscriber(), source);
//                    subscriber.shuffleGrouping(source, stream);
//                }
//                it.remove();
//            } else {
//                log.error("No subscriber found for id '{}'", subscription.subscriber());
//            }
//        }
//
//        if (!st.subscriptions.isEmpty()) {
//            log.info("Unresolved subscriptions: {}", st.subscriptions);
//            throw new Exception("Found " + st.subscriptions.size() + " unresolved subscription references!");
//        }
//
//        return st;
//    }

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

//    public void addBolt(String id, BoltDeclarer bolt) {
//        bolts.put(id, bolt);
//    }
}