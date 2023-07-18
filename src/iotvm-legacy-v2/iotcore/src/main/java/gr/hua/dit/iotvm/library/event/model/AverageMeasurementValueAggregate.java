package gr.hua.dit.iotvm.library.event.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate for calculating and holding the average of measurement values.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt Saturday 14 January 2023
 * @since 1.0.0-PROTOTYPE.1
 */
public class AverageMeasurementValueAggregate extends BaseEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Map<String, SensorTelemetryMeasurementEvent> sensorTelemetryMeasurementEvents;
    private Double average;
    private Long timestamp;

    /* ------------ Constructors ------------ */

    public AverageMeasurementValueAggregate() {
        this.sensorTelemetryMeasurementEvents = new HashMap<>();
        this.average = Double.MIN_VALUE;
        this.timestamp = Instant.now().toEpochMilli();
    }

    /* ------------ Getters ------------ */

    public Map<String, SensorTelemetryMeasurementEvent> getSensorTelemetryMeasurementEvents() {
        return sensorTelemetryMeasurementEvents;
    }

    public Double getAverage() {
        return average;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    /* ------------ Modifiers ------------ */

    public void addSensorTelemetryMeasurementEvent(SensorTelemetryMeasurementEvent sensorTelemetryMeasurementEvent) {
        if (this.sensorTelemetryMeasurementEvents.containsKey(sensorTelemetryMeasurementEvent.getNodeId())) {
            if (sensorTelemetryMeasurementEvent.getTimestamp()
                    >= this.sensorTelemetryMeasurementEvents
                            .get(sensorTelemetryMeasurementEvent.getNodeId())
                            .getTimestamp()) {
                this.sensorTelemetryMeasurementEvents.put(
                        sensorTelemetryMeasurementEvent.getNodeId(), sensorTelemetryMeasurementEvent);
            }
        } else {
            this.sensorTelemetryMeasurementEvents.put(
                    sensorTelemetryMeasurementEvent.getNodeId(), sensorTelemetryMeasurementEvent);
        }
        double sum = this.sensorTelemetryMeasurementEvents.values().stream()
                .map(SensorTelemetryMeasurementEvent::getValue)
                .reduce(Double::sum)
                .orElse(0D);
        double total = this.sensorTelemetryMeasurementEvents.values().size();
        if (total > 0) {
            this.average = sum / total;
        } else {
            this.average = Double.MIN_VALUE;
        }
        this.timestamp = Instant.now().toEpochMilli();
    }

    /**
     * @param name the name or the ID of the sensor.
     * @return {@code true} if map contains value for the specified sensor, otherwise {@code false}.
     */
    public boolean isSensorIncluded(String name) {
        return this.sensorTelemetryMeasurementEvents.containsKey(name);
    }

    /* ------------ Overrides ------------ */

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("AverageMeasurementValueAggregate{");
        sb.append("sensorTelemetryMeasurementEvents=").append(sensorTelemetryMeasurementEvents);
        sb.append(", average=").append(average);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }
}
