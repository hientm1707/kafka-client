package vn.edu.hcmut.consumer;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import kafka.tools.ConsoleConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
=import org.apache.kafka.clients.producer.ProducerRecord;
import vn.edu.hcmut.configs.MyJsonDeserializer;
import vn.edu.hcmut.invoice.Invoice;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class LogConsumer {

    private List<String> SUBSCRIBE_TOPICS = Arrays.asList("topic1", "topic2");
    public LogConsumer(){
        final var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "validation-consumer");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(MyJsonDeserializer.VALUE_CLASS_NAME_CONFIG, Invoice.class);

        final var consumer = new KafkaConsumer<String, Invoice>(consumerProps);
        consumer.subscribe(SUBSCRIBE_TOPICS);

        final var producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "validation-producer");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyJsonDeserializer.class);

        final var producer = new KafkaProducer<>(producerProps);

        while (true) {
            final var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(r -> {
                if (!r.value().isValid()) {
                    producer.send(new ProducerRecord<>("invalid-invoice-topic"), r.value().getStoreId(), r.value());
                    log.info("Invalid record - {}", r.value().getInvoiceNumber());
                    return;
                }
                producer.send(new ProducerRecord<>("valid-invoice-topic", r.value().getStoreId(), r.value()));
                log.info("Valid record - {}", r.value().getInvoiceNumber());
            });
        }

    }

}
