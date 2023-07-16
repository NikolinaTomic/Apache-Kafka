package com.kafka.springbootkafkaproducer.producer;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.kafka.springbootkafkaproducer.model.TransactionEvent;
//import lombok.NonNull;
//import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.crypto.KeyGenerator;
import java.util.UUID;

@Component
public class TransactionEventProducer {
    @Autowired
    KafkaTemplate<String, Transaction> kafkaTemplate;
    private static final String TOPIC = "transaction-amount-topic";

    public void send(TransactionEvent transactionEvent) {
        kafkaTemplate.send(createEventMessage(transactionEvent, UUID.randomUUID().toString()));
    }

    //TransactionEvent
    private Message<TransactionEvent> createEventMessage(TransactionEvent event, String key) {
        return MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader(KafkaHeaders.KEY, key)
                .build();
    }
}
