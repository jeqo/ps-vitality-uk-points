package uk.vitality.points.steps.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class JsonSerde<T> implements Serde<T> {
    final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<>(type);
    }

    static class JsonSerializer<T> implements Serializer<T> {
        ObjectMapper jsonMapper = new ObjectMapper();

        JsonSerializer() {
            jsonMapper.registerModule(new JavaTimeModule());
            jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Override
        public byte[] serialize(String s, T json) {
            try {
                return jsonMapper.writeValueAsBytes(json);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Json serialization failed", e);
            }
        }
    }

    static class JsonDeserializer<T> implements Deserializer<T> {
        ObjectMapper jsonMapper = new ObjectMapper();

        final Class<T> type;

        JsonDeserializer(Class<T> type) {
            this.type = type;

            jsonMapper.registerModule(new JavaTimeModule());
            jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Override
        public T deserialize(String s, byte[] bytes) {
            try {
                return jsonMapper.readValue(bytes, type);
            } catch (IOException e) {
                throw new IllegalStateException("Json deserialization failed", e);
            }
        }
    }
}
