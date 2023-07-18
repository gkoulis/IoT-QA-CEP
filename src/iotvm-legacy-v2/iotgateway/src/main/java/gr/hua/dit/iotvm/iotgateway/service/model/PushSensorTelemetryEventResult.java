package gr.hua.dit.iotvm.iotgateway.service.model;

import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import java.io.Serial;
import java.io.Serializable;

/**
 * Result object after pushing a {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public class PushSensorTelemetryEventResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private SensorTelemetryEvent sensorTelemetryEvent;

    /* ------------ Constructors ------------ */

    public PushSensorTelemetryEventResult() {
        this.sensorTelemetryEvent = null;
    }

    /* ------------ Getters and Setters ------------ */

    public SensorTelemetryEvent getSensorTelemetryEvent() {
        return sensorTelemetryEvent;
    }

    public void setSensorTelemetryEvent(SensorTelemetryEvent sensorTelemetryEvent) {
        this.sensorTelemetryEvent = sensorTelemetryEvent;
    }

    /* ------------ Overrides ------------ */

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("PushSensorTelemetryEventResult{");
        sb.append("sensorTelemetryEvent=").append(sensorTelemetryEvent);
        sb.append('}');
        return sb.toString();
    }
}
