package com.kafka.springbootkafkaproducer.controller;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.kafka.springbootkafkaproducer.producer.TransactionEventProducer;
import com.kafka.springbootkafkaproducer.worker.CSVWorker;
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
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@RestController
@RequestMapping("transactions")
public class TransactionController {

    private final String filePath = "../../../Transactions.csv";
    @Autowired
    TransactionEventProducer transactionEventProducer;

    @PostMapping
    public void sendTransactions() throws IOException, CsvValidationException, InterruptedException {
        int numThreads = 2; // Number of worker threads

        // Create a blocking queue to hold CSV data
        BlockingQueue<String[]> queue = new LinkedBlockingQueue<>();

        // Create and start worker threads
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.execute(new CSVWorker(queue, transactionEventProducer));
        }

        CSVReader reader =
                new CSVReaderBuilder(new FileReader(filePath)).build();
        String[] line;
        // Read CSV file and add lines to the queue
        while ((line = reader.readNext()) != null) {
            queue.put(line);
        }

        // Signal worker threads to stop after queue is processed
        executor.shutdown();
        try {
            // Wait for all tasks to complete or timeout after a certain duration
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("Forcing shutdown after timeout...");
                executor.shutdownNow(); // Interrupt running threads
            }
        } catch (InterruptedException e) {
            executor.shutdownNow(); // Re-interrupt the thread if it was interrupted
        }
        System.out.println("Main thread finished.");

//                          One thread
//        CSVReader reader =
//                new CSVReaderBuilder(new FileReader(filePath)).build();
//        var fields = reader.readNext();
//        while (fields != null) {
//            var transaction = new Transaction(UUID.fromString(fields[0]),
//                    UUID.fromString(fields[1]), UUID.fromString(fields[2]), new BigDecimal(fields[3]), fields[4]);
//            transactionEventProducer.send(new TransactionEvent(UUID.randomUUID(), transaction));
//            fields = reader.readNext();
//        }
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
