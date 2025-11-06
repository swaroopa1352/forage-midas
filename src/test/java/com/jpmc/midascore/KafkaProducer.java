package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import java.util.Map;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void init() {
        System.out.println("[diagnostic] KafkaTemplate class: " + this.kafkaTemplate.getClass().getName());
        try {
            Object pf = this.kafkaTemplate.getProducerFactory();
            System.out.println("[diagnostic] ProducerFactory class: " + (pf == null ? "null" : pf.getClass().getName()));
            if (pf instanceof DefaultKafkaProducerFactory) {
                Map<String, Object> props = ((DefaultKafkaProducerFactory<?, ?>) pf).getConfigurationProperties();
                System.out.println("[diagnostic] ProducerFactory config: " + props);
                System.out.println("[diagnostic] value.serializer: " + props.get("value.serializer"));
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void send(String transactionLine) {
        String[] transactionData = transactionLine.split(", ");
        Float amount = Float.parseFloat(transactionData[2]);
        System.out.println("[Transaction Amount] " + amount);
        kafkaTemplate.send(topic, new Transaction(Long.parseLong(transactionData[0]), Long.parseLong(transactionData[1]), amount));
    }
}