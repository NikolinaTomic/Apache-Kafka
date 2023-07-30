package com.kafka.springbootkafkaconsumer.consumer;

import com.kafka.springbootkafkaproducer.model.TransactionEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TransactionEventConsumer {

    @KafkaListener(topics = "transaction-amount-topic", groupId = "group_json",
            containerFactory = "transactionKafkaListenerFactory")
    public void consume(TransactionEvent event) {
        System.out.println("Uid : " + event.getTransaction().getUid());
        System.out.println("Amount : " + event.getTransaction().getAmount());
        System.out.println("From : " + event.getTransaction().getFromAccount());
        System.out.println("To : " + event.getTransaction().getToAccount());
        System.out.println("Time : " + event.getTransaction().getTransactionDateTime());
        System.out.println("==========================");
    }

}
