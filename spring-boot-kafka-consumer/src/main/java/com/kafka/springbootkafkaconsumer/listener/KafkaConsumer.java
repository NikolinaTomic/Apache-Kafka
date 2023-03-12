package com.kafka.springbootkafkaconsumer.listener;

import com.kafka.springbootkafkaproducer.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    //@KafkaListener(topics = "kafka_example", groupId = "group_id")
    //public void consume(String message) {
    //    System.out.println("Consumed message: " + message);
    //}

    @KafkaListener(topics = "kafka_example", groupId = "group_id",
            containerFactory = "userKafkaListenerFactory")
    public void consumeJson(User user) {
        System.out.println("Consumed JSON message: " + user);
    }

}
