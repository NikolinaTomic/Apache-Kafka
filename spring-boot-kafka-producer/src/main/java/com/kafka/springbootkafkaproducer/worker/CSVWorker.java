package com.kafka.springbootkafkaproducer.worker;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.kafka.springbootkafkaproducer.model.TransactionEvent;
import com.kafka.springbootkafkaproducer.producer.TransactionEventProducer;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class CSVWorker implements Runnable {
    private final BlockingQueue<String[]> queue;

    @Autowired
    TransactionEventProducer transactionEventProducer;
    
    public CSVWorker(BlockingQueue<String[]> queue, TransactionEventProducer transactionEventProducer) {
        this.queue = queue;
        this.transactionEventProducer = transactionEventProducer;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String[] fields = queue.take(); // Get a line from the queue
                var transaction = new Transaction(UUID.fromString(fields[0]),
                        UUID.fromString(fields[1]), UUID.fromString(fields[2]), new BigDecimal(fields[3]), fields[4]);
                transactionEventProducer.send(new TransactionEvent(UUID.randomUUID(), transaction));
            }
        } catch (InterruptedException e) {
            // Thread interrupted, exiting...
        }
    }
}
