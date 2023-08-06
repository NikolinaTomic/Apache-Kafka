package com.kafka.springbootkafkaconsumer.consumer;

import com.kafka.springbootkafkaproducer.model.TransactionEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.UUID;

@Service
public class TransactionEventConsumer {

    private final HashMap<UUID, BigDecimal> transactionsEventHashMap = new HashMap<>();
    private int counter = 0;
    private long start;
    private long end;

    @KafkaListener(topics = "transaction-amount-topic", groupId = "group_json",
            containerFactory = "transactionKafkaListenerFactory")
    public void consume(TransactionEvent event) {
        if (counter == 0) {
            start = System.currentTimeMillis();
        }
        counter++;

        System.out.println("Uid : " + event.getTransaction().getUid());
        System.out.println("Amount : " + event.getTransaction().getAmount());
        System.out.println("From : " + event.getTransaction().getFromAccount());
        System.out.println("To : " + event.getTransaction().getToAccount());
        System.out.println("Time : " + event.getTransaction().getTransactionDateTime());
        System.out.println("==========================");

        var amount = event.getTransaction().getAmount();
        var sender = event.getTransaction().getFromAccount();
        if (transactionsEventHashMap.containsKey(sender)) {
            var value = transactionsEventHashMap.get(sender);
            value = value.subtract(amount);
            transactionsEventHashMap.put(sender, value);
        } else {
            transactionsEventHashMap.put(sender, amount.negate());
        }
        var receiver = event.getTransaction().getToAccount();
        if (transactionsEventHashMap.containsKey(receiver)) {
            var value = transactionsEventHashMap.get(receiver);
            value = value.add(amount);
            transactionsEventHashMap.put(receiver, value);
        } else {
            transactionsEventHashMap.put(receiver, amount);
        }

        if (counter == 5) {
            end = System.currentTimeMillis();
            System.out.println("Elapsed time: " + (end - start));
            for (var key : transactionsEventHashMap.keySet()) {
                System.out.println("Key: " + key + ", value: " + transactionsEventHashMap.get(key));
            }
        }

    }

}
