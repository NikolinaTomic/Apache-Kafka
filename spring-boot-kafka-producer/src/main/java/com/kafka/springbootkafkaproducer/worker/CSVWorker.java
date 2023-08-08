package com.kafka.springbootkafkaproducer.worker;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.kafka.springbootkafkaproducer.model.UserTransaction;
import com.kafka.springbootkafkaproducer.model.UserTransactionEvent;
import com.kafka.springbootkafkaproducer.model.UserType;
import com.kafka.springbootkafkaproducer.producer.TransactionEventProducer;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

@AllArgsConstructor
public class CSVWorker implements Runnable {
    private final BlockingQueue<String[]> queue;

    @Autowired
    TransactionEventProducer transactionEventProducer;

    @Override
    public void run() {
        try {
            while (true) {
                String[] fields = queue.take(); // Get a line from the queue
                var transaction = Transaction.builder()
                        .uid(UUID.fromString(fields[0]))
                        .fromAccount(UUID.fromString(fields[1]))
                        .toAccount(UUID.fromString(fields[2]))
                        .amount(new BigDecimal(fields[3]))
                        .transactionDateTime(fields[4])
                        .build();

                // Double entry
                var buyerUserTransaction = UserTransaction.builder()
                        .uid(transaction.getUid())
                        .userType(UserType.RECIPIENT)
                        .account(transaction.getToAccount())
                        .amount(transaction.getAmount())
                        .build();
                transactionEventProducer.send(
                        UserTransactionEvent.builder()
                                .eventUid(UUID.randomUUID())
                                .userTransaction(buyerUserTransaction)
                                .build());

                var sellerUserTransaction = UserTransaction.builder()
                        .uid(transaction.getUid())
                        .userType(UserType.SENDER)
                        .account(transaction.getFromAccount())
                        .amount(transaction.getAmount())
                        .build();
                transactionEventProducer.send(
                        UserTransactionEvent.builder()
                                .eventUid(UUID.randomUUID())
                                .userTransaction(sellerUserTransaction)
                                .build());
            }
        } catch (InterruptedException e) {
            // Thread interrupted, exiting...
        }
    }
}
