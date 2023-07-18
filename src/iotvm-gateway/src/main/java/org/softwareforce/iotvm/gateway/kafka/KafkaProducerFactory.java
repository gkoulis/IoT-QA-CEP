package org.softwareforce.iotvm.gateway.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Factory for providing {@link org.apache.kafka.clients.producer.KafkaProducer}.
 *
 * @author Dimitris Gkoulis
 * @deprecated in favor of a better implementation that supports configuration from file, singleton,
 *     safe initialization, and more.
 */
@Deprecated
public final class KafkaProducerFactory {

  private final Properties producerProperties;
  private final KafkaProducer<String, SpecificRecord> kafkaProducer;

  public KafkaProducerFactory() {
    // TODO Load from file.
    this.producerProperties = new Properties();
    this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:10101");
    this.producerProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
    this.producerProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    this.producerProperties.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:10102");
    this.producerProperties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    this.producerProperties.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false);
    // this.producerProperties.put(ProducerConfig.ACKS_CONFIG, "0");
    // this.producerProperties.put(ProducerConfig.RETRIES_CONFIG, "0");
    // this.producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    // this.producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
    // this.producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
    this.kafkaProducer = new KafkaProducer<>(this.producerProperties);
  }

  public KafkaProducer<String, SpecificRecord> getKafkaProducer() {
    return kafkaProducer;
  }
}
