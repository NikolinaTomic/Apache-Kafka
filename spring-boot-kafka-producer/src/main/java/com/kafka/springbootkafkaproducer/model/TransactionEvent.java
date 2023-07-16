package com.kafka.springbootkafkaproducer.model;

//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.springframework.kafka.event.KafkaEvent;

import java.util.UUID;

public class TransactionEvent {//implements KafkaEvent {

    private UUID eventUid;
    private Transaction transaction;

    public TransactionEvent(UUID eventUid, Transaction transaction) {
        this.eventUid = eventUid;
        this.transaction = transaction;
    }
}
