package vn.edu.hcmut.consumer;


import config.Invoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import config.MyJsonDeserializer;
import config.MyJsonSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;



public class MyConsumer {

    private static List<String> SUBSCRIBE_TOPICS = Arrays.asList("invoice-topic");

    public static void main(String... args) {
        final var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "validation-consumer");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyJsonDeserializer.class);
        consumerProps.put(MyJsonDeserializer.VALUE_CLASS_NAME_CONFIG, Invoice.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id1");
        final var consumer = new KafkaConsumer<String, Invoice>(consumerProps);

        consumer.subscribe(SUBSCRIBE_TOPICS);
        final var producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "validation-producer");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyJsonSerializer.class);

        final var producer = new KafkaProducer<>(producerProps);

        while (true) {
            final var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(r -> {
                if (!r.value().isValid()) {
                    producer.send(new ProducerRecord<>("invalid-invoice-topic", r.value().getStoreId(), r.value()));
                    System.out.println("Invalid record - " + r.value().getInvoiceNumber());
                    return;
                }
                producer.send(new ProducerRecord<>("valid-invoice-topic", r.value().getStoreId(), r.value()));
                System.out.println("Valid record - " + r.value().getInvoiceNumber());
            });
        }
    }


}

