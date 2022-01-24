package config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

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
        if (Objects.isNull(data)) {
            return null;
        }
        try {
            return objectMapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
