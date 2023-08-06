package com.kafka.springbootkafkaproducer.controller;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.kafka.springbootkafkaproducer.model.TransactionEvent;
import com.kafka.springbootkafkaproducer.producer.TransactionEventProducer;
import com.opencsv.bean.*;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
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

    @Autowired
    TransactionEventProducer transactionEventProducer;

    private final String filePath = "../../../Transactions.csv";

    @PostMapping
    public void sendTransactions() throws IOException {
        try (
                Reader reader = new FileReader(filePath);
        ) {
            CsvToBean<Transaction> csvToBean = new CsvToBeanBuilder<Transaction>(reader)
                    .withType(Transaction.class)
                    .withSeparator(',')
                    .withIgnoreLeadingWhiteSpace(true)
                    .build();

            List<Transaction> transactions = csvToBean.parse();
            transactions.stream()
                    .map(transaction -> new TransactionEvent(UUID.randomUUID(), transaction))
                    .forEach(transactionEvent -> transactionEventProducer.send(transactionEvent));
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
        int jlimit = 10000;
        int j = 0;
        while (j < jlimit) {
            List<Transaction> transactions = new ArrayList<>();
            int limit = 1000;
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
