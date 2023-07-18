package software.dgk.mozart.prototype1.usecase1;

import java.time.Instant;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * A producer for generating random device events with a defined schema.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 * @future TODO Produce late or out of order event
 */
@Component
public class UC1RandomSensorGenericEventProducer {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  public UC1RandomSensorGenericEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /* ------------ Private ------------ */

  private int getRandomInteger(int min, int max) {
    return (int) ((Math.random() * (max - min)) + min);
  }

  private double getRandomDouble(double min, double max) {
    return (double) ((Math.random() * (max - min)) + min);
  }

  /* ------------ Scheduled Tasks ------------ */

  @Scheduled(fixedDelay = 1000)
  public void generate() {
    final UC1DeviceSpecificEvent e =
        new UC1DeviceSpecificEvent(
            String.valueOf(getRandomInteger(1, 6)),
            getRandomDouble(20, 30),
            "CELSIUS",
            getRandomDouble(0, 100),
            "PERCENTAGE",
            Instant.now().toEpochMilli());
    // System.out.println("Generating message: " + e); // TODO Use log.
    // ProducerRecord<String, UC1DeviceSpecificEvent> producerRecord = new
    // ProducerRecord<>(UC1Constants.SENSOR_TELEMETRY_EVENT_TOPIC, e);
    this.kafkaTemplate.send(UC1Constants.SENSOR_TELEMETRY_EVENT_TOPIC, e);
  }
}
