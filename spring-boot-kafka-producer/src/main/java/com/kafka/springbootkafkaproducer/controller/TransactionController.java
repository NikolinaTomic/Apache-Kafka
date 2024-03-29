package com.kafka.springbootkafkaproducer.controller;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.kafka.springbootkafkaproducer.model.UserTransaction;
import com.kafka.springbootkafkaproducer.model.UserTransactionEvent;
import com.kafka.springbootkafkaproducer.model.UserType;
import com.kafka.springbootkafkaproducer.producer.UserTransactionEventProducer;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("transactions")
public class TransactionController {

    private final String filePath = "../../../TransactionsTest.csv";
    @Autowired
    UserTransactionEventProducer userTransactionEventProducer;

    @PostMapping
    public void sendTransactions() throws IOException, CsvValidationException, InterruptedException {
        CSVReader reader =
                new CSVReaderBuilder(new FileReader(filePath)).build();
        var fields = reader.readNext();
        while (fields != null) {
            var transaction = Transaction.builder()
                    .uid(UUID.fromString(fields[0]))
                    .fromAccount(UUID.fromString(fields[1]))
                    .toAccount(UUID.fromString(fields[2]))
                    .amount(new BigDecimal(fields[3]))
                    .transactionDateTime(fields[4])
                    .build();

            // Double entry accounting
            var buyerUserTransaction = UserTransaction.builder()
                    .uid(transaction.getUid())
                    .userType(UserType.RECIPIENT)
                    .account(transaction.getToAccount())
                    .amount(transaction.getAmount())
                    .build();
            userTransactionEventProducer.send(
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
            userTransactionEventProducer.send(
                    UserTransactionEvent.builder()
                            .eventUid(UUID.randomUUID())
                            .userTransaction(sellerUserTransaction)
                            .build());

            fields = reader.readNext();
        }
    }

    @PostMapping("/generate")
    public void generateFile() throws IOException, CsvRequiredFieldEmptyException, CsvDataTypeMismatchException {
        var file = Paths.get(filePath);
        OpenOption openOption = StandardOpenOption.CREATE;
        ColumnPositionMappingStrategy mappingStrategy =
                new ColumnPositionMappingStrategy();
        mappingStrategy.setType(Transaction.class);
        String[] columns = new String[]
                {"uid", "fromAccount", "toAccount", "amount", "transactionDateTime"};
        mappingStrategy.setColumnMapping(columns);

        var list = Transaction.setUUIDList();
        int jlimit = 1;
        int j = 0;
        while (j < jlimit) {
            List<Transaction> transactions = new ArrayList<>();
            int limit = 3000;
            int i = 0;
            while (i < limit) {
                var transaction = new Transaction(list);
                transactions.add(transaction);
                i++;
            }

            if (Files.exists(file)) {
                openOption = StandardOpenOption.APPEND;
            }
            Writer writer = Files.newBufferedWriter(file, openOption);

            // Creating StatefulBeanToCsv object
            StatefulBeanToCsvBuilder<Transaction> builder =
                    new StatefulBeanToCsvBuilder(writer);
            StatefulBeanToCsv beanWriter =
                    builder.withMappingStrategy(mappingStrategy).build();
            // Write list to StatefulBeanToCsv object
            beanWriter.write(transactions);
            writer.close();
            j++;
        }
        // closing the writer object
    }
}
