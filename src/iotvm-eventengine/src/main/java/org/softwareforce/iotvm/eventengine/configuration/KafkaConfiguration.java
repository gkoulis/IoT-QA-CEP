package org.softwareforce.iotvm.eventengine.configuration;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Kafka client and Kafka Streams configuration.
 *
 * @author Dimitris Gkoulis
 */
public final class KafkaConfiguration {

  /**
   * The Kafka Streams application ID. The ID, aka application name, must be unique in the Kafka
   * cluster againist which the application is run.
   */
  private static final String APPLICATION_ID = "greenhouse-application-1";

  private static final String CLIENT_ID = "greenhouse-application-kafka-client";
  private static final String BOOTSTRAP_SERVERS = "localhost:10101";
  // private static final String STATE_DIRECTORY =
  // "F:\\projects\\PhD\\DGk-PhD-Monorepo\\src\\iotvm-local-data\\iotvm-eventengine\\kafka-streams";
  // Γιατί στα windows έχω όριο στο path.
  private static final String STATE_DIRECTORY = "F:\\tmp";
  public static final String SCHEMA_REGISTRY_URL = "http://localhost:10102";

  /* ------------ Constructors ------------ */

  public KafkaConfiguration() {}

  /* ------------ Beans ------------ */

  public Admin getKafkaAdmin() {
    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    return Admin.create(properties);
  }

  public Properties getKafkaStreamsProperties() {
    final Properties properties = new Properties();

    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    // Where to find the Confluent schema registry instance(s)
    // properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    // Specify default (de)serializers for record keys and for record values.
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    // properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    // Producer Serialization and Deserialization configuration.
    properties.put(
        StreamsConfig.producerPrefix(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
        StringSerializer.class);
    properties.put(
        StreamsConfig.producerPrefix(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
        KafkaAvroSerializer.class);

    // Consumer Serialization and Deserialization configuration.
    properties.put(
        StreamsConfig.consumerPrefix(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),
        StringDeserializer.class);
    properties.put(
        StreamsConfig.consumerPrefix(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),
        KafkaAvroDeserializer.class);

    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
    properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    properties.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false);

    properties.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIRECTORY);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    // Processing Guarantees.
    // properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    // Records should be flushed every 10 seconds.
    // This is less than the default in order to keep the demonstration interactive.
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

    return properties;
  }
}
