package com.solopov.learn;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
public class LearnKafkaApplication {

    private static final String TOPIC_HOBBIT = "hobbit";
    private static final String TOPIC_HOBBIT_2 = "hobbit_2";


    public static void main(String[] args) {
        SpringApplication.run(LearnKafkaApplication.class, args);
    }

    /**
     * The definition of this bean has invoked programmatic creation of this topic in Kafka
     */
    @Bean
    NewTopic hobbit2() {
        return TopicBuilder.name(TOPIC_HOBBIT_2)
                .partitions(12)
                .replicas(3)
                .build();
    }


    @Component
    @RequiredArgsConstructor
    class Producer {

        private final KafkaTemplate<Integer, String> template;

        private final Faker faker = Faker.instance();


        @EventListener(ApplicationStartedEvent.class)
        public void generate() {
            Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
            Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));
            Flux.zip(interval, quotes).map(
                    it -> template.send(TOPIC_HOBBIT, faker.random().nextInt(42), it.getT2())
            ).blockLast();
        }
    }


    @Component
    @RequiredArgsConstructor
    class Consumer {

        @KafkaListener(topics = {TOPIC_HOBBIT}, groupId = "spring-boot-kafka")
        public void consume(ConsumerRecord<Integer, String> record) {
            System.out.printf("received key=%d value= %s%n", record.key(), record.value());
        }
    }
}
