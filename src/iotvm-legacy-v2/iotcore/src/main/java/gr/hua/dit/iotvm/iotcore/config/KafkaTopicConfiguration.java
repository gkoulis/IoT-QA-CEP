package gr.hua.dit.iotvm.iotcore.config;

import gr.hua.dit.iotvm.iotcore.config.Constants.MeasurementType;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

/**
 * Configures Kafka topics.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt Saturday 14 January 2023
 * @since 1.0.0-PROTOTYPE.1
 */
@Configuration
public class KafkaTopicConfiguration {

    @Bean
    public NewTopics newKafkaTopics() {
        final List<String> topicNameList = new ArrayList<>();
        topicNameList.add(Constants.SENSOR_TELEMETRY_EVENT_TOPIC);
        topicNameList.add(Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC);
        topicNameList.add(MeasurementType.TEMPERATURE.getSensorTelemetryMeasurementEventTopic());
        topicNameList.add(MeasurementType.HUMIDITY.getSensorTelemetryMeasurementEventTopic());
        topicNameList.add(Constants.WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_TOPIC);
        topicNameList.add(MeasurementType.TEMPERATURE.getWindowedAverageMeasurementValueEventTopic());
        topicNameList.add(MeasurementType.HUMIDITY.getWindowedAverageMeasurementValueEventTopic());

        return new NewTopics(topicNameList.stream()
                .map((name) -> TopicBuilder.name(name).partitions(1).replicas(1).build())
                .toArray(NewTopic[]::new));
    }
}
