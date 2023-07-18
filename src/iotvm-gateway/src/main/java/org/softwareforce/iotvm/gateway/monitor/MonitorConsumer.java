package org.softwareforce.iotvm.gateway.monitor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.SseBroadcaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.gateway.configuration.Constants;
import org.softwareforce.iotvm.gateway.kafka.KafkaConsumerFactory;
import org.softwareforce.iotvm.gateway.kafka.KafkaSerDe;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;

/**
 * Kafka consumer for monitoring.
 *
 * @author Dimitris Gkoulis
 */
public final class MonitorConsumer {

  private final Logger LOGGER = LoggerFactory.getLogger(MonitorConsumer.class);

  /** Internal {@link ObjectMapper} for converting JSON {@link String} to {@link Map}. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Internal {@link TypeReference} for {@link #OBJECT_MAPPER}. */
  @SuppressWarnings("Convert2Diamond")
  private static final TypeReference<Map<String, Object>> TYPE_REFERENCE =
      new TypeReference<Map<String, Object>>() {};

  private final OutboundSseEvent.Builder outboundSseEventBuilder;
  private final SseBroadcaster sseBroadcaster;
  private Thread internalThread;

  /* ------------ Constructors ------------ */

  public MonitorConsumer(
      OutboundSseEvent.Builder outboundSseEventBuilder, SseBroadcaster sseBroadcaster) {
    this.outboundSseEventBuilder = outboundSseEventBuilder;
    this.sseBroadcaster = sseBroadcaster;
    this.internalThread = null;
  }

  /* ------------ Serialization / Deserialization ------------ */

  /**
   * @deprecated Deprecated in favor of {@link io.confluent.kafka.serializers.KafkaAvroDeserializer}
   *     and {@link KafkaSerDe}. Not working because it misses information from Kafka message
   *     (topic, headers, value [bytes composition], etc.)
   */
  @Deprecated
  public static <T extends SpecificRecordBase> T fromBytes(byte[] bytes, Class<T> tClass)
      throws IOException {
    final DatumReader<T> reader = new SpecificDatumReader<>(tClass);
    final ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
    final Decoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
    return reader.read(null, decoder);
  }

  public static <T extends SpecificRecordBase> String toJSON(T obj) throws IOException {
    final DatumWriter<T> writer = new SpecificDatumWriter<>(obj.getSchema());
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(obj.getSchema(), stream);
    writer.write(obj, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString(StandardCharsets.UTF_8);
  }

  public static Map<String, Object> toMap(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, TYPE_REFERENCE);
  }

  /* ------------ Logic ------------ */

  public void initAndStart() {
    if (this.internalThread != null) {
      if (this.internalThread.isAlive()) {
        throw new IllegalStateException("internalThread is already alive!");
      }
    }
    this.internalThread = new Thread(new InternalMonitorConsumer());
    this.internalThread.start();
  }

  /* ------------ Runnable ------------ */

  private final class InternalMonitorConsumer implements Runnable {

    public InternalMonitorConsumer() {}

    @Override
    public void run() {
      final KafkaConsumer<String, byte[]> kafkaConsumer = KafkaConsumerFactory.get("monitor");

      final Deserializer<SensorTelemetryMeasurementEventIBO>
          sensorTelemetryMeasurementEventIBODeserializer =
              KafkaSerDe.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE.deserializer();
      final Deserializer<SensorTelemetryMeasurementsAverageEventIBO>
          sensorTelemetryMeasurementsAverageEventIBODeserializer =
              KafkaSerDe.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE.deserializer();

      // final String SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC_TEMPERATURE =
      // Constants.getSensorTelemetryMeasurementEventTopic(PhysicalQuantity.TEMPERATURE);
      // final String SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC_HUMIDITY =
      // Constants.getSensorTelemetryMeasurementEventTopic(PhysicalQuantity.HUMIDITY);

      kafkaConsumer.subscribe(
          List.of(
              Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC,
              Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC));

      while (true) {
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, byte[]> record : records) {
          final String topicName = record.topic();
          String id = "id-" + System.currentTimeMillis(); // Temp.
          final MonitoringEvent monitoringEvent = new MonitoringEvent(topicName, null);

          switch (topicName) {
            case Constants.SENSOR_TELEMETRY_RAW_EVENT_TOPIC:
            case Constants.SENSOR_TELEMETRY_EVENT_TOPIC:
              break;
            case Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC:
              try {
                final SensorTelemetryMeasurementEventIBO ibo =
                    sensorTelemetryMeasurementEventIBODeserializer.deserialize(
                        record.topic(), record.headers(), record.value());
                final String iboAsJson = toJSON(ibo);
                final Map<String, Object> iboAsMap = toMap(iboAsJson);
                monitoringEvent.setReal(iboAsMap);
              } catch (Exception ex) {
                LOGGER.error("Failed to extract SensorTelemetryMeasurementEventIBO!", ex);
                monitoringEvent.setReal(null);
              }
              break;
            case Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC:
              try {
                final SensorTelemetryMeasurementsAverageEventIBO ibo =
                    sensorTelemetryMeasurementsAverageEventIBODeserializer.deserialize(
                        record.topic(), record.headers(), record.value());
                final String iboAsJson = toJSON(ibo);
                final Map<String, Object> iboAsMap = toMap(iboAsJson);
                monitoringEvent.setReal(iboAsMap);
              } catch (Exception ex) {
                LOGGER.error("Failed to extract SensorTelemetryMeasurementsAverageEventIBO!", ex);
                monitoringEvent.setReal(null);
              }
              break;
            default:
              break;
          }

          final OutboundSseEvent outboundSseEvent =
              outboundSseEventBuilder
                  .name("monitoringEvent")
                  .id(id)
                  .mediaType(MediaType.APPLICATION_JSON_TYPE)
                  .data(MonitoringEvent.class, monitoringEvent)
                  .reconnectDelay(10_000)
                  .comment("new monitoring event triggered by event in topic: " + topicName)
                  .build();

          sseBroadcaster.broadcast(outboundSseEvent);
        }
      }
    }
  }
}
