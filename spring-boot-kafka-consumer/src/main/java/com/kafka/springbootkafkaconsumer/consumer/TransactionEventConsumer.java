package com.kafka.springbootkafkaconsumer.consumer;

import com.kafka.springbootkafkaproducer.model.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TransactionEventConsumer {

    @KafkaListener(topics = "transaction-amount-topic", groupId = "group_id",
            containerFactory = "transactionConsumerFactory")
    public void consume(Transaction transaction) {
        System.out.println("Uid : " + transaction.getUid());
        System.out.println("Amount : " + transaction.getAmount());
        System.out.println("From : " + transaction.getFromAccount());
        System.out.println("To : " + transaction.getToAccount());
        System.out.println("Time : " + transaction.getTransactionDateTime());
        System.out.println("==========================");
    }

}
