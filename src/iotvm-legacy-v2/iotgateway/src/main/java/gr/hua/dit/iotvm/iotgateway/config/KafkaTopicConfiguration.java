package gr.hua.dit.iotvm.iotgateway.config;

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
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Configuration
public class KafkaTopicConfiguration {

    @Bean
    public NewTopics newKafkaTopics() {
        final List<String> topicNameList = new ArrayList<>();
        topicNameList.add(Constants.SENSOR_TELEMETRY_EVENT_TOPIC);

        return new NewTopics(topicNameList.stream()
                .map((name) -> TopicBuilder.name(name).partitions(1).replicas(1).build())
                .toArray(NewTopic[]::new));
    }
}
