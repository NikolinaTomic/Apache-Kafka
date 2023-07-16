package com.kafka.springbootkafkaconsumer;

import com.kafka.springbootkafkaproducer.model.Transaction;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class SpringBootKGenerateTransactionCsvApplication {

    public static void main(String[] args) throws IOException, CsvRequiredFieldEmptyException, CsvDataTypeMismatchException {
        var list = Transaction.setUUIDList();
        List<Transaction> transactions = new ArrayList<>();
        int limit = 5;
        int i = 0;
        while (i < limit) {
            var transaction = new Transaction(list);
            transactions.add(transaction);
            i++;
        }
//
//        try (FileWriter writer = new FileWriter("../../Transactions.csv")) {
//            String[] header = new String[]
//                { "uid", "fromAccount", "toAccount", "amount", "transactionDateTime" };
//            ColumnPositionMappingStrategy mappingStrategy = new ColumnPositionMappingStrategy();
//            mappingStrategy.setType(Transaction.class);
//            mappingStrategy.setColumnMapping(header);
//
//            StatefulBeanToCsv<Transaction> beanToCsv = new StatefulBeanToCsvBuilder<Transaction>(writer)
//                    .withMappingStrategy(mappingStrategy)
//                    .withSeparator('#')
//                    .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
//                    .build();
//
//            // todo: getting empty csv file. why? currently debugging.
//            beanToCsv.write(transactions);
//            for (Transaction x : transactions) {
//                System.out.println(x.getUid().toString());
//            }
//        } catch (CsvRequiredFieldEmptyException e) {
//            e.printStackTrace();
//        } catch (CsvDataTypeMismatchException e) {
//            e.printStackTrace();
//        }


        ////////////////////////////2. naci
        FileWriter writer = new FileWriter("../../Transactions.csv");
        ColumnPositionMappingStrategy mappingStrategy =
                new ColumnPositionMappingStrategy();
        mappingStrategy.setType(Transaction.class);
        String[] columns = new String[]
                {"uid", "fromAccount", "toAccount", "amount", "transactionDateTime"};
        mappingStrategy.setColumnMapping(columns);

        // Creating StatefulBeanToCsv object
        StatefulBeanToCsvBuilder<Transaction> builder =
                new StatefulBeanToCsvBuilder(writer);
        StatefulBeanToCsv beanWriter =
                builder.withMappingStrategy(mappingStrategy).build();

        // Write list to StatefulBeanToCsv object
        beanWriter.write(transactions);

        // closing the writer object
        writer.close();
    }

}
