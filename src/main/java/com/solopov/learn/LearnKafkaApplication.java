package com.solopov.learn;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;

@SpringBootApplication
@EnableKafkaStreams
public class LearnKafkaApplication {

    private static final String TOPIC_HOBBIT = "hobbit";
    private static final String TOPIC_HOBBIT_2 = "hobbit_2";
    private static final String TOPIC_WORD_COUNT = "streams-wordcount-output";

    private static final String MATERIALIZED_NAME_COUNTS = "counts";


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

    @Bean
    NewTopic counts() {
        return TopicBuilder.name(TOPIC_WORD_COUNT)
                .partitions(6)
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

        @KafkaListener(topics = {TOPIC_WORD_COUNT}, groupId = "spring-boot-kafka")
        public void consume(ConsumerRecord<String, Long> record) {
            System.out.printf("received key=%s value= %d%n", record.key(), record.value());
        }
    }


    @Component
    class Processor {

        @Autowired
        public void process(StreamsBuilder builder) {
            // Serializers/deserializers (serde) for Integer, String types
            final Serde<Integer> integerSerde = Serdes.Integer();
            final Serde<String> stringSerde = Serdes.String();
            final Serde<Long> longSerde = Serdes.Long();

            // Construct a KStream from the input topic (f.e. 'hobbit'), where message values represent
            // lines of text (for the sake of this example, we ignore whatever may be stores in the message keys).
            KStream<Integer, String> textLines = builder.stream(TOPIC_HOBBIT, Consumed.with(integerSerde, stringSerde));

            KTable<String, Long> wordCounts = textLines
                    // Split each text line, by whitespace, into words. The text lines are the message values, i.e., we
                    // can ignore whatever data is in the message keys and thus invoke 'flatMapValues' instead of the
                    // more generic 'flatMap'
                    .flatMapValues(values -> Arrays.asList(values.toLowerCase().split("\\W+")))
                    // We use 'groupBy' to ensure the words are available as message keys
                    .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
                    // Count the occurrences of each word (message key).
                    .count(Materialized.as(MATERIALIZED_NAME_COUNTS));

            // Convert the 'KTable<String, Long>' into a 'KStream<String, Long>' and write the output topic.
            wordCounts.toStream().to(TOPIC_WORD_COUNT, Produced.with(stringSerde, longSerde));
        }
    }


    @RestController
    @RequiredArgsConstructor
    class RestService {

        private final StreamsBuilderFactoryBean factoryBean;


        @GetMapping("/count/{word}")
        public Long getCount(@PathVariable String word) {
            KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

            ReadOnlyKeyValueStore<Object, Long> counts = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            MATERIALIZED_NAME_COUNTS,
                            QueryableStoreTypes.keyValueStore()
                    )
            );
            return counts.get(word);
        }
    }
}
