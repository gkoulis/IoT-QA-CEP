package gr.hua.dit.iotvm.iotcore.demonstration;

import gr.hua.dit.iotvm.iotcore.config.Constants;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEventMeasurement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Random {@link SensorTelemetryEvent} generator.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt Saturday 14 January 2023
 * @since 1.0.0-PROTOTYPE.1
 */
@Component
@Profile("generator")
public class Generator {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /* ------------ Constructors ------------ */

    public Generator(
            @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
                    KafkaTemplate<String, Object> kafkaTemplate) {
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

    @Scheduled(fixedDelay = 5000)
    public void generate() {
        final List<SensorTelemetryEventMeasurement> sensorTelemetryEventMeasurementList = List.of(
                new SensorTelemetryEventMeasurement("temperature", getRandomDouble(15, 25), "CELSIUS"),
                new SensorTelemetryEventMeasurement("humidity", getRandomDouble(5, 95), "PERCENTAGE"));
        final int nodeNumber = getRandomInteger(1, 6);
        final String nodeId = "node_" + nodeNumber;
        String nodeGroupId = "nodeGroup_";
        if (nodeNumber == 1 || nodeNumber == 2) {
            nodeGroupId = nodeGroupId + "1";
        } else if (nodeNumber == 3 || nodeNumber == 4) {
            nodeGroupId = nodeGroupId + "2";
        } else if (nodeNumber == 5 || nodeNumber == 6) {
            nodeGroupId = nodeGroupId + "3";
        } else {
            nodeGroupId = nodeGroupId + "invalid";
        }

        final SensorTelemetryEvent sensorTelemetryEvent = new SensorTelemetryEvent(
                sensorTelemetryEventMeasurementList,
                nodeId,
                nodeGroupId,
                Instant.now().toEpochMilli(),
                new HashMap<>());
        this.kafkaTemplate.send(Constants.SENSOR_TELEMETRY_EVENT_TOPIC, sensorTelemetryEvent);
    }
}
