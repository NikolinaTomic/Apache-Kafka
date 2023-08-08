package com.kafka.springbootkafkaproducer.producer;

import com.kafka.springbootkafkaproducer.model.UserTransactionEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class TransactionEventProducer {
    private static final String TOPIC = "transaction-amount-topic";
    @Autowired
    KafkaTemplate<String, UserTransactionEvent> kafkaTemplate;

    public void send(UserTransactionEvent userTransactionEvent) {
        kafkaTemplate.send(createEventMessage(userTransactionEvent, UUID.randomUUID().toString()));
    }

    private Message<UserTransactionEvent> createEventMessage(UserTransactionEvent event, String key) {
        return MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader(KafkaHeaders.KEY, key)
                .build();
    }
}
