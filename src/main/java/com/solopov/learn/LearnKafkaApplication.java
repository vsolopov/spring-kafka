package com.solopov.learn;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
public class LearnKafkaApplication {

    private static final String TOPIC_HOBBIT = "hobbit";


    public static void main(String[] args) {
        SpringApplication.run(LearnKafkaApplication.class, args);
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
        public void consume(String quote) {
            System.out.printf("received = %s%n", quote);
        }
    }
}
