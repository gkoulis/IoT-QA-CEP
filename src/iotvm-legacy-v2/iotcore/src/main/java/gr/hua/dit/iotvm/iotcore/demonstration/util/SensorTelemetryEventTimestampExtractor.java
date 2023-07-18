package gr.hua.dit.iotvm.iotcore.demonstration.util;

import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TimestampExtractor} for {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 14 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public class SensorTelemetryEventTimestampExtractor implements TimestampExtractor {

    private static final Logger logger = LoggerFactory.getLogger(SensorTelemetryEventTimestampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimestamp) {
        try {
            Long timestamp = ((SensorTelemetryEvent) consumerRecord.value()).getTimestamp();
            return TimestampExtractorUtil.getTimestamp(timestamp, previousTimestamp);
        } catch (Exception ex) {
            logger.error("Failed to extract timestamp from SensorTelemetryEvent", ex);
            return TimestampExtractorUtil.getTimestamp(null, previousTimestamp);
        }
    }
}
