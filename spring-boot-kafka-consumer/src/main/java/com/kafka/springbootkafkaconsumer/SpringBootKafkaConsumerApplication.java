package com.kafka.springbootkafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootKafkaConsumerApplication {

    public static void main(String[] args) {
//        try (
//                Reader reader = new FileReader("../../Transactions.csv")
//        ) {
//            CsvToBean<Transaction> csvToBean = new CsvToBeanBuilder(reader)
//                    .withType(Transaction.class)
//                    .withSeparator(',')
//                    .withIgnoreLeadingWhiteSpace(true)
//                    .build();
//
//            Iterator<Transaction> transactionIterator = csvToBean.iterator();
//            while (transactionIterator.hasNext()) {
//                Transaction transaction = transactionIterator.next();
//                System.out.println("Uid : " + transaction.getUid());
//                System.out.println("Amount : " + transaction.getAmount());
//                System.out.println("From : " + transaction.getFromAccount());
//                System.out.println("To : " + transaction.getToAccount());
//                System.out.println("Time : " + transaction.getTransactionDateTime());
//                System.out.println("==========================");
//            }
//        }

        SpringApplication.run(SpringBootKafkaConsumerApplication.class, args);
    }
}
