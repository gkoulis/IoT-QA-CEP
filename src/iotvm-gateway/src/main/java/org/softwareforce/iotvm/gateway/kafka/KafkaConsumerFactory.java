package org.softwareforce.iotvm.gateway.kafka;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Factory for providing {@link org.apache.kafka.clients.consumer.KafkaConsumer}.
 *
 * @author Dimitris Gkoulis
 * @deprecated in favor of a better implementation that supports configuration from file, singleton,
 *     safe initialization, and more.
 */
@Deprecated
public final class KafkaConsumerFactory {

  /* ------------ Constructors ------------ */

  private KafkaConsumerFactory() {}

  /* ------------ Factory Methods ------------ */

  @SuppressWarnings("Convert2Diamond")
  public static KafkaConsumer<String, byte[]> get(final String groupId) {
    final Properties properties = new Properties();
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    // TODO TEMPORARY: use latest!
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<String, byte[]>(properties);
  }
}
