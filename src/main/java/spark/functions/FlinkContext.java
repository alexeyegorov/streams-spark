package spark.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import stream.Context;
import stream.ProcessContext;

/**
 * @author chris, alexey
 */
public class FlinkContext implements ProcessContext, Serializable {
    /** The unique class ID */
    private static final long serialVersionUID = 6162013508460469957L;
    static Logger log = LoggerFactory.getLogger(FlinkContext.class);

    final Map<String, Serializable> values = new LinkedHashMap<>();
    transient Map<String, Object> volatileValues = new LinkedHashMap<>();

    /**
     * Create bolt context using given id. Application ID can be set using 'set(...)' method.
     *
     * @param id UUID of a process
     */
    public FlinkContext(String id) {
        set("process", (!id.equals(""))? id : UUID.randomUUID().toString());
    }

    /**
     * @see stream.Context#resolve(java.lang.String)
     */
    @Override
    public Object resolve(String variable) {
        if (variable.startsWith("application.id")) {
            return get(variable);
        }
        if (!variable.startsWith("process.") || !variable.startsWith("application.id")) {
            log.error("A BoltContext does currently not provide following contexts: " + variable);
            return null;
        }

        return get(variable.substring("process.".length()));
    }

    /**
     * @see stream.ProcessContext#get(java.lang.String)
     */
    @Override
    public Object get(String key) {

        if (values.containsKey(key)) {
            return values.get(key);
        }
        if (volatileValues.containsKey(key)) {
            return volatileValues.get(key);
        }

        log.debug("No value for key '{}' stored in this context.", key);
        return null;
    }

    /**
     * @see stream.ProcessContext#set(java.lang.String, java.lang.Object)
     */
    @Override
    public void set(String key, Object o) {

        if (o instanceof Serializable) {
            values.put(key, (Serializable) o);
            volatileValues.remove(key);
            return;
        }

        log.warn("Storing non-serializable object in context! The object might be lost during outages!");
        values.remove(key);
        volatileValues.put(key, o);
    }

    public Object readResolve() {
        if (this.volatileValues == null) {
            volatileValues = new LinkedHashMap<>();
        }
        return this;
    }

    public boolean contains(String key) {
        return values.containsKey(key) || volatileValues.containsKey(key);
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub
    }

    @Override
    public String getId() {
        return (String) get("process");
    }

    @Override
    public Context getParent() {
        return null;
    }

    @Override
    public String path() {
        return get("application.id").toString() + "/" + get("process").toString();
    }
}
