package config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class MyJsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Object o) {
        if (o == null) {
            return new byte[0];
        }
        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new SerializationException("Error serializing  JSON message", e);
        }
    }
}
