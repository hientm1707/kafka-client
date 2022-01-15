package vn.edu.hcmut.configs;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class MyJsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> clazz;
    public static final String KEY_CLASS_NAME_CONFIG = "key.class.name";
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";


    @Override
    public void configure(Map configs, boolean isKey) {
        if (isKey) {
            clazz = (Class<T>) configs.get(KEY_CLASS_NAME_CONFIG);
            return;
        }
        clazz = (Class<T>) configs.get(VALUE_CLASS_NAME_CONFIG);
    }


    @Override
    public T deserialize(String topic, byte[] data) {
        if (Objects.isNull(data)){
            return null;
        }
        try {
            return objectMapper.readValue(data, clazz);
        } catch (Exception e) {
            log.info("Could not deserialize data with data");
            throw new RuntimeException(e);
        }
    }
}
