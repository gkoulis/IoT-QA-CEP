package software.dgk.mozart.prototype1.usecase1;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Configuration for the <i>Use Case 1</i>.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt Wednesday 16 February 2022
 * @since 1.0.0-PROTOTYPE.1
 */
@Configuration
public class UC1Configuration {

  @Bean
  public NewTopic sensorTelemetryEventTopic() {
    return TopicBuilder.name(UC1Constants.SENSOR_TELEMETRY_EVENT_TOPIC)
        .partitions(1)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic temperatureUpdateEventTopic() {
    return TopicBuilder.name(UC1Constants.TEMPERATURE_UPDATE_EVENT_TOPIC)
        .partitions(1)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic moistureUpdateEventTopic() {
    return TopicBuilder.name(UC1Constants.MOISTURE_UPDATE_EVENT_TOPIC)
        .partitions(1)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic humidityUpdateEventTopic() {
    return TopicBuilder.name(UC1Constants.HUMIDITY_UPDATE_EVENT_TOPIC)
        .partitions(1)
        .replicas(1)
        .build();
  }

  // TODO Create dynamically...
  @Bean
  public NewTopic uc1TemperatureUpdateEvent1() {
    return TopicBuilder.name("uc1-temperature-update-event-1").partitions(1).replicas(1).build();
  }
}
