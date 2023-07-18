package gr.hua.dit.iotvm.library.event.model;

import gr.hua.dit.iotvm.library.Constants;
import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A meaningful event with an average measurement value calculation information.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 14 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class WindowedAverageMeasurementValueEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String microServiceId;
    private final Long windowStartTimeEpochMilli;
    private final Long windowEndTimeEpochMilli;
    private final String measurementName;
    private final String measurementUnit;
    private final Map<String, SensorTelemetryMeasurementEvent> sensorTelemetryMeasurementEvents;
    private final Double average;
    private final Long timestamp;
    private final Long timeWindowDurationInSeconds;
    private final Long timeWindowAdvanceInSeconds;
    private final Integer numberOfContributingNodes;
    private final Integer minimumNumberOfContributingNodes;

    /* ------------ Constructors ------------ */

    /** Constructor for Jackson. */
    public WindowedAverageMeasurementValueEvent() {
        this.microServiceId = Constants.UNDEFINED;
        this.windowStartTimeEpochMilli = Long.MIN_VALUE;
        this.windowEndTimeEpochMilli = Long.MIN_VALUE;
        this.measurementName = Constants.UNDEFINED;
        this.measurementUnit = Constants.UNDEFINED;
        this.sensorTelemetryMeasurementEvents = new HashMap<>();
        this.average = Double.MIN_VALUE;
        this.timestamp = Long.MIN_VALUE;
        this.timeWindowDurationInSeconds = Long.MIN_VALUE;
        this.timeWindowAdvanceInSeconds = Long.MIN_VALUE;
        this.numberOfContributingNodes = Integer.MIN_VALUE;
        this.minimumNumberOfContributingNodes = Integer.MIN_VALUE;
    }

    public WindowedAverageMeasurementValueEvent(
            String microServiceId,
            Long windowStartTimeEpochMilli,
            Long windowEndTimeEpochMilli,
            String measurementName,
            String measurementUnit,
            Map<String, SensorTelemetryMeasurementEvent> sensorTelemetryMeasurementEvents,
            Double average,
            Long timestamp,
            Long timeWindowDurationInSeconds,
            Long timeWindowAdvanceInSeconds,
            Integer numberOfContributingNodes,
            Integer minimumNumberOfContributingNodes) {
        this.microServiceId = microServiceId;
        this.windowStartTimeEpochMilli = windowStartTimeEpochMilli;
        this.windowEndTimeEpochMilli = windowEndTimeEpochMilli;
        this.measurementName = measurementName;
        this.measurementUnit = measurementUnit;
        this.sensorTelemetryMeasurementEvents = sensorTelemetryMeasurementEvents;
        this.average = average;
        this.timestamp = timestamp;
        this.timeWindowDurationInSeconds = timeWindowDurationInSeconds;
        this.timeWindowAdvanceInSeconds = timeWindowAdvanceInSeconds;
        this.numberOfContributingNodes = numberOfContributingNodes;
        this.minimumNumberOfContributingNodes = minimumNumberOfContributingNodes;
    }

    /* ------------ Getters ------------ */

    public String getMicroServiceId() {
        return microServiceId;
    }

    public Long getWindowStartTimeEpochMilli() {
        return windowStartTimeEpochMilli;
    }

    public Long getWindowEndTimeEpochMilli() {
        return windowEndTimeEpochMilli;
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public String getMeasurementUnit() {
        return measurementUnit;
    }

    public Map<String, SensorTelemetryMeasurementEvent> getSensorTelemetryMeasurementEvents() {
        return sensorTelemetryMeasurementEvents;
    }

    public Double getAverage() {
        return average;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Long getTimeWindowDurationInSeconds() {
        return timeWindowDurationInSeconds;
    }

    public Long getTimeWindowAdvanceInSeconds() {
        return timeWindowAdvanceInSeconds;
    }

    public Integer getNumberOfContributingNodes() {
        return numberOfContributingNodes;
    }

    public Integer getMinimumNumberOfContributingNodes() {
        return minimumNumberOfContributingNodes;
    }

    /* ------------ Overrides ------------ */

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("WindowedAverageMeasurementValueEvent{");
        sb.append("microServiceId='").append(microServiceId).append('\'');
        sb.append(", windowStartTimeEpochMilli=").append(windowStartTimeEpochMilli);
        sb.append(", windowEndTimeEpochMilli=").append(windowEndTimeEpochMilli);
        sb.append(", measurementName='").append(measurementName).append('\'');
        sb.append(", measurementUnit='").append(measurementUnit).append('\'');
        sb.append(", sensorTelemetryMeasurementEvents=").append(sensorTelemetryMeasurementEvents);
        sb.append(", average=").append(average);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", timeWindowDurationInSeconds=").append(timeWindowDurationInSeconds);
        sb.append(", timeWindowAdvanceInSeconds=").append(timeWindowAdvanceInSeconds);
        sb.append(", numberOfContributingNodes=").append(numberOfContributingNodes);
        sb.append(", minimumNumberOfContributingNodes=").append(minimumNumberOfContributingNodes);
        sb.append('}');
        return sb.toString();
    }
}
