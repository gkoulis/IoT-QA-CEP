package software.dgk.mozart.prototype1.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import software.dgk.mozart.prototype1.usecase1.UC1GenericEvent;

/**
 * Configuration for Kafka and Kafka streams.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "greenhouseAutomationApp");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // TODO Create custom JsonSerde. (?)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
    // configure the state location to allow tests to use clean state for every run
    // props.put(STATE_DIR_CONFIG, stateStoreLocation);

    props.put(StreamsConfig.producerPrefix(JsonSerializer.ADD_TYPE_INFO_HEADERS), true);
    props.put(StreamsConfig.consumerPrefix(JsonDeserializer.KEY_DEFAULT_TYPE), String.class);
    // TODO Use Jackson JsonNode.
    props.put(
        StreamsConfig.consumerPrefix(JsonDeserializer.VALUE_DEFAULT_TYPE), UC1GenericEvent.class);
    props.put(
        StreamsConfig.consumerPrefix(JsonDeserializer.TRUSTED_PACKAGES),
        "software.dgk.mozart.prototype1.usecase1");

    props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
    props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
    // TODO Use Jackson JsonNode.
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, UC1GenericEvent.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "software.dgk.mozart.prototype1.usecase1");

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

    return new KafkaStreamsConfiguration(props);
  }
}
