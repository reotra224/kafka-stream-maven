import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamConsumerApp {

    public static void main(String[] args) {
        new KafkaStreamConsumerApp().start();
    }

    private void start() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-id-1");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kStream = streamsBuilder.stream("ab1Topic",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, Long> resultStream = kStream.flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                .map((k, v) -> new KeyValue<>(k, v.toLowerCase()))
                //.filter((k, v) -> !v.isEmpty())
                .filter((k, v) -> v.equals("a") || v.equals("b"))
                .groupBy((k, v) -> v)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .count(Materialized.as("count-analytics"))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.window().startTime() + " --- " + k.window().endTime() + " -> " + k.key(), v));
                // .foreach(((k, v) -> System.out.println("Key => " + k + " : Value => " + v)));

        // resultStream.to("resultTopic", Produced.with(Serdes.String(), Serdes.String()));
        resultStream.to("result1Topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();

    }
}



















