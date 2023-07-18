package gr.hua.dit.iotvm.iotcore.demonstration;

import gr.hua.dit.iotvm.library.event.model.SensorTelemetryMeasurementEvent;
import java.util.Optional;

public class SensorTelemetryMeasurementEventDefaultStorageAdapter implements StorageAdapter<SensorTelemetryMeasurementEvent> {
    @Override
    public void insert(SensorTelemetryMeasurementEvent value) {
        // TODO assign ID (client-side?)!
    }

    @Override
    public Optional<SensorTelemetryMeasurementEvent> getTimely() {
        return Optional.empty();
    }

    @Override
    public void delete(SensorTelemetryMeasurementEvent value) {
    }
}
