package com.kafka.springbootkafkaconsumer.consumer;

import com.kafka.springbootkafkaproducer.model.TransactionEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

@Service
public class TransactionEventConsumer {

    private final HashMap<UUID, BigDecimal> transactionsEventHashMap = new HashMap<>();
    private int counter = 0;
    private Instant start;
    private Instant end;

    @KafkaListener(topics = "transaction-amount-topic", groupId = "group_json",
            containerFactory = "transactionKafkaListenerFactory")
    public void consume(TransactionEvent event) {
        if (counter == 0) {
            start = Instant.now();
        }
        counter++;

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

        if (counter % 10000 == 0) {
            end = Instant.now();
            System.out.println("Num: " + counter);
            System.out.println("Elapsed time seconds: " + Duration.between(start, end).toSeconds());
            System.out.println("Elapsed time nanoseconds: " + Duration.between(start, end).toNanos());
            System.out.println("==========================");
            // write map to csv file after processing
        }
    }

}
