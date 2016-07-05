package edu.jhu.fcriscu1.taskframework.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import lombok.extern.log4j.Log4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by fcriscuolo on 7/5/16.
 */
@Log4j
public enum PropertiesService {
    INSTANCE;

    private Map<String,String> propertiesMap = Suppliers.memoize(new PropertiesMapSupplier()).get();

    public Optional<String> getStringPropertyByName(String aName){
       if(Strings.isNullOrEmpty(aName) || !this.propertiesMap.containsKey(aName)) {
           return Optional.empty();
       }
        return  Optional.of(propertiesMap.get(aName));
    }

    public Optional<Integer> getIntegerPropertyByName(String aName){
     return this.getStringPropertyByName(aName).flatMap((s)->
          Optional.of(Integer.valueOf(s)));
    }

    public Optional<Long> getLongPropertyByName(String aName){
        return this.getStringPropertyByName(aName).flatMap((s)->
                Optional.of(Long.valueOf(s)));
    }

    public static void main(String[] args) {
        //orphan.default.number.task.requests=500
        PropertiesService.INSTANCE
                .getIntegerPropertyByName("orphan.default.number.task.requests")
                .ifPresent((i) ->
                log.info("Orphan number of tasks=" +i));
        PropertiesService.INSTANCE.getLongPropertyByName("orphan.default.task.max.processing.duration")
                .ifPresent((l)-> log.info("Max processing duration =" +l));
        PropertiesService.INSTANCE.getStringPropertyByName("no.such.property")
                .ifPresent((s)-> log.info("No such property =" +s));

    }

    private class PropertiesMapSupplier implements Supplier<Map<String,String>>{
        private final String propertiesFileName = "/framework.properties";
        private Map<String,String> propertiesMap;

        PropertiesMapSupplier() {
            this.initializePropertiesMap();
        }
        private void initializePropertiesMap() {
            Properties properties = new Properties();
           try( InputStream in = getClass().getResourceAsStream(propertiesFileName)) {
               properties.load(in);
               this.propertiesMap =properties.entrySet().stream().collect
                       (Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
           } catch (IOException e) {
               log.error(e.getMessage());
           }
        }


        @Override
        public Map<String, String> get() {
            return this.propertiesMap;
        }
    }
}
