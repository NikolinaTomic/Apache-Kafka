package com.kafka.springbootkafkaproducer.model;

import com.opencsv.bean.CsvBindByPosition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {
    @CsvBindByPosition(position = 0)
    private UUID uid;
    @CsvBindByPosition(position = 1)
    private UUID fromAccount;
    @CsvBindByPosition(position = 2)
    private UUID toAccount;
    @CsvBindByPosition(position = 3)
    private BigDecimal amount;
    @CsvBindByPosition(position = 4)
    private String transactionDateTime;

    public Transaction(List<UUID> uuidList) {
        Random random = new Random();
        UUID randomFromAccount = uuidList.get(random.nextInt(uuidList.size()));
        UUID randomToAccount = uuidList.get(random.nextInt(uuidList.size()));

        uid = UUID.randomUUID();
        fromAccount = randomFromAccount;
        toAccount = randomToAccount;
        amount = generateRandomBigDecimalFromRange(BigDecimal.valueOf(100), BigDecimal.valueOf(1000000));
        transactionDateTime = LocalDateTime.now().toString();
    }

    public static List<UUID> setUUIDList() {
        return Stream.generate(UUID::randomUUID)
                .limit(100)
                .collect(Collectors.toList());
    }

    public static BigDecimal generateRandomBigDecimalFromRange(BigDecimal min, BigDecimal max) {
        BigDecimal randomBigDecimal = min.add(BigDecimal.valueOf(Math.random()).multiply(max.subtract(min)));
        return randomBigDecimal.setScale(2, RoundingMode.HALF_UP);
    }
}
