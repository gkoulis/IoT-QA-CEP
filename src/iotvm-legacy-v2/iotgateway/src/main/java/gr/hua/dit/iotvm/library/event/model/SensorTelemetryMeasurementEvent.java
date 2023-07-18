package gr.hua.dit.iotvm.library.event.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A sensor telemetry measurement event with one measurement.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class SensorTelemetryMeasurementEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;
    private final Double value;
    private final String unit;
    private final String nodeId;
    private final String nodeGroupId;
    private final Long timestamp;
    private final Map<String, Object> extraData;

    /* ------------ Constructors ------------ */

    /** Constructor for Jackson. */
    public SensorTelemetryMeasurementEvent() {
        this.name = "undefined";
        this.value = 0D;
        this.unit = "undefined";
        this.nodeId = "undefined";
        this.nodeGroupId = "undefined";
        this.timestamp = 0L;
        this.extraData = new HashMap<>();
    }

    public SensorTelemetryMeasurementEvent(
            String name,
            Double value,
            String unit,
            String nodeId,
            String nodeGroupId,
            Long timestamp,
            Map<String, Object> extraData) {
        this.name = name;
        this.value = value;
        this.unit = unit;
        this.nodeId = nodeId;
        this.nodeGroupId = nodeGroupId;
        this.timestamp = timestamp;
        this.extraData = extraData;
    }

    /* ------------ Getters ------------ */

    public String getName() {
        return name;
    }

    public Double getValue() {
        return value;
    }

    public String getUnit() {
        return unit;
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
        final StringBuffer sb = new StringBuffer("SensorTelemetryMeasurementEvent{");
        sb.append("name='").append(name).append('\'');
        sb.append(", value=").append(value);
        sb.append(", unit='").append(unit).append('\'');
        sb.append(", nodeId='").append(nodeId).append('\'');
        sb.append(", nodeGroupId='").append(nodeGroupId).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", extraData=").append(extraData);
        sb.append('}');
        return sb.toString();
    }
}
