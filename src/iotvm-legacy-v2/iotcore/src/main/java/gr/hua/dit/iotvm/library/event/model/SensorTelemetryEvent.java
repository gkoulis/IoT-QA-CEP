package gr.hua.dit.iotvm.library.event.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A sensor telemetry event with multiple measurements.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class SensorTelemetryEvent extends BaseEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final List<SensorTelemetryEventMeasurement> measurements;
    private final String nodeId;
    private final String nodeGroupId;
    private final Long timestamp;
    private final Map<String, Object> extraData;

    /* ------------ Constructors ------------ */

    /** Constructor for Jackson.  */
    public SensorTelemetryEvent() {
        this.measurements = new ArrayList<>();
        this.nodeId = "undefined";
        this.nodeGroupId = "undefined";
        this.timestamp = 0L;
        this.extraData = new HashMap<>();
    }

    public SensorTelemetryEvent(
            List<SensorTelemetryEventMeasurement> measurements,
            String nodeId,
            String nodeGroupId,
            Long timestamp,
            Map<String, Object> extraData) {
        this.measurements = measurements;
        this.nodeId = nodeId;
        this.nodeGroupId = nodeGroupId;
        this.timestamp = timestamp;
        this.extraData = extraData;
    }

    /* ------------ Getters ------------ */

    public List<SensorTelemetryEventMeasurement> getMeasurements() {
        return measurements;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getExtraData() {
        return extraData;
    }

    /* ------------ Overrides ------------ */

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SensorTelemetryEvent{");
        sb.append("measurements=").append(measurements);
        sb.append(", nodeId='").append(nodeId).append('\'');
        sb.append(", nodeGroupId='").append(nodeGroupId).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", extraData=").append(extraData);
        sb.append('}');
        return sb.toString();
    }
}
