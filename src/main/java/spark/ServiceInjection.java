package spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import spark.functions.SparkService;
import stream.Processor;
import stream.runtime.DependencyInjection;
import stream.runtime.setup.factory.ProcessorFactory;

/**
 * Inject service into a processor using any service.
 *
 * @author alexey
 */
public class ServiceInjection implements ProcessorFactory.ProcessorCreationHandler {

    static Logger log = LoggerFactory.getLogger(ServiceInjection.class);

    /**
     * List of SparkService used as wrapper for real service implementations.
     */
    private final List<SparkService> sparkServices;

    public ServiceInjection(List<SparkService> sparkServices) {
        this.sparkServices = sparkServices;
    }

    @Override
    public void processorCreated(Processor p, Element from) throws Exception {
        // collect all declared field (even from superclasses)
        List<Field> fields = getDeclaredFields(p);

        // iterate through declared fields and search for a service-field.
        for (Field field : fields) {

            if (DependencyInjection.isServiceImplementation(field.getType())) {
                log.info("Checking service-field {}", field.getName());

                String serviceName = field.getName();
                stream.annotations.Service sa = field.getAnnotation(stream.annotations.Service.class);

                // if annotation contains 'name' then use this name instead of fiel name
                // (service can be named through XML configuration
                if (sa != null && !sa.name().isEmpty()) {
                    serviceName = sa.name();
                }

                log.info("Service field '{}' relates to property '{}' for processor {}",
                        field.getName(), serviceName, p);

                try {
                    boolean accessible = field.isAccessible();
                    field.setAccessible(true);

                    SparkService sparkService = getSparkService(serviceName);
                    if (sparkService != null) {
                        log.debug("Injecting   '{}'.{}   <-- " + sparkService, p, serviceName);
                        field.set(p, sparkService.getService());
                    } else {
                        log.error("StormService with name {} were not found.", serviceName);
                    }

                    field.setAccessible(accessible);
                } catch (IllegalAccessException e) {
                    log.error("Field {} could not have been set", serviceName);
                }

            }
        }
    }

    /**
     * Collect declared field from current class and its superclasses.
     *
     * @param processor processor with declared fields
     * @return list of found declared fields
     */
    private List<Field> getDeclaredFields(Processor processor) {
        //TODO what if several superclasses?
        Field[] declaredFields = processor.getClass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(0);
        fields.addAll(Arrays.asList(declaredFields));
        Class<?> serv = processor.getClass();
        while (serv.getSuperclass() != Object.class) {
            Class<?> superclass = serv.getSuperclass();
            Field[] declaredFields1 = superclass.getDeclaredFields();
            List<Field> fields1 = Arrays.asList(declaredFields1);
            fields.addAll(fields1);
            serv = superclass;
        }
        return fields;
    }

    /**
     * Iterate through the list of all services and find a service with the given name.
     *
     * @param name service name
     * @return SparkService
     */
    private SparkService getSparkService(String name) {
        for (SparkService service : sparkServices) {
            if (service.getServiceName().equals(name)) {
                return service;
            }
        }
        return null;
    }
}
