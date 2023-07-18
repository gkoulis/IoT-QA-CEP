package gr.hua.dit.iotvm.iotcore.demonstration.util;

import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Pre-processing Kafka Streams {@link Processor} for cleaning, enriching and correcting {@link SensorTelemetryEvent} instances.
 *
 * <p>Features:
 * <ul>
 *     <li>If {@code timestamp} is {@code null}, set {@code timestamp} of {@link Record}. Notice that {@link Record} has always a valid timestamp because consumer uses {@link SensorTelemetryEventTimestampExtractor} that always returns a valid timestamp.</li>
 * </ul>
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 16 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public class PreProcessingProcessor implements Processor<String, SensorTelemetryEvent, String, SensorTelemetryEvent> {

    private ProcessorContext<String, SensorTelemetryEvent> context;

    @Override
    public void init(ProcessorContext<String, SensorTelemetryEvent> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, SensorTelemetryEvent> record) {
        SensorTelemetryEvent sensorTelemetryEvent = record.value();
        if (sensorTelemetryEvent.getTimestamp() == null) {
            // TODO Use setters!
            sensorTelemetryEvent = new SensorTelemetryEvent(
                    sensorTelemetryEvent.getMeasurements(),
                    sensorTelemetryEvent.getNodeId(),
                    sensorTelemetryEvent.getNodeGroupId(),
                    record.timestamp(),
                    sensorTelemetryEvent.getExtraData());
            record = record.withValue(sensorTelemetryEvent);
        }
        this.context.forward(record);
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
