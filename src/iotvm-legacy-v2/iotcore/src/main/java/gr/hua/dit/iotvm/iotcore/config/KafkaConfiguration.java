package gr.hua.dit.iotvm.iotcore.config;

import gr.hua.dit.iotvm.library.event.model.AverageMeasurementValueAggregate;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryMeasurementEvent;
import gr.hua.dit.iotvm.library.event.model.WindowedAverageMeasurementValueEvent;
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
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Configuration for Kafka and Kafka streams.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {

    private static final String PACKAGE_NAME = "gr.hua.dit.iotvm.library.event.model";

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        final Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "greenhouseAutomationApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        props.put(StreamsConfig.producerPrefix(JsonSerializer.ADD_TYPE_INFO_HEADERS), true);
        // props.put(StreamsConfig.producerPrefix(JsonSerializer.TYPE_MAPPINGS), "");

        props.put(StreamsConfig.consumerPrefix(JsonDeserializer.KEY_DEFAULT_TYPE), String.class);
        // props.put(StreamsConfig.consumerPrefix(JsonDeserializer.VALUE_DEFAULT_TYPE), ?.class);
        props.put(StreamsConfig.consumerPrefix(JsonDeserializer.TRUSTED_PACKAGES), PACKAGE_NAME);
        props.put(StreamsConfig.consumerPrefix(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS), false);

        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);

        props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
        // props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ?.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, PACKAGE_NAME);
        props.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        return new KafkaStreamsConfiguration(props);
    }

    // @Bean
    public RecordMessageConverter converter() {
        JsonMessageConverter converter = new JsonMessageConverter();
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
        typeMapper.addTrustedPackages(PACKAGE_NAME);
        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("AverageMeasurementValueAggregate", AverageMeasurementValueAggregate.class);
        mappings.put("SensorTelemetryEvent", SensorTelemetryEvent.class);
        mappings.put("SensorTelemetryMeasurementEvent", SensorTelemetryMeasurementEvent.class);
        mappings.put("WindowedAverageMeasurementValueEvent", WindowedAverageMeasurementValueEvent.class);
        typeMapper.setIdClassMapping(mappings);
        converter.setTypeMapper(typeMapper);
        return converter;
    }
}
