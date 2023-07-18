package org.softwareforce.iotvm.eventengine.persistence.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Simple Wrapper for persisted IBO entities.
 *
 * @author Dimitris Gkoulis
 */
public class PersistedIBO {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistedIBO.class);

  /** Internal {@link ObjectMapper} for converting {@link #recordJSON} to {@link #real}. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Internal {@link TypeReference} for {@link #OBJECT_MAPPER}. */
  @SuppressWarnings("Convert2Diamond")
  private static final TypeReference<Map<String, Object>> TYPE_REFERENCE =
      new TypeReference<Map<String, Object>>() {};

  /* ------------ Properties ------------ */

  /** The MongoDB ID. */
  @Nullable @BsonId private ObjectId id;

  @Nullable private String topicName;

  /** The avro schema as JSON. */
  @Nullable private String schemaJSON;

  /** The avro specific record as JSON. */
  @Nullable private String recordJSON;

  /** The avro data represented as map. */
  @Nullable private Map<String, Object> real;

  /* ------------ Constructors ------------ */

  /** Empty constructor for serialization/deserialization purposes. */
  public PersistedIBO() {
    this.id = null;
    this.topicName = null;
    this.schemaJSON = null;
    this.recordJSON = null;
    this.real = null;
  }

  public PersistedIBO(
      @Nullable ObjectId id,
      @Nullable String topicName,
      @Nullable String schemaJSON,
      @Nullable String recordJSON,
      @Nullable Map<String, Object> real) {
    this.id = id;
    this.topicName = topicName;
    this.schemaJSON = schemaJSON;
    this.recordJSON = recordJSON;
    this.real = real;
  }

  /* ------------ Getters and Setters ------------ */

  @Nullable
  public ObjectId getId() {
    return id;
  }

  public void setId(@Nullable ObjectId id) {
    this.id = id;
  }

  @Nullable
  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(@Nullable String topicName) {
    this.topicName = topicName;
  }

  @Nullable
  public String getSchemaJSON() {
    return schemaJSON;
  }

  public void setSchemaJSON(@Nullable String schemaJSON) {
    this.schemaJSON = schemaJSON;
  }

  @Nullable
  public String getRecordJSON() {
    return recordJSON;
  }

  public void setRecordJSON(@Nullable String recordJSON) {
    this.recordJSON = recordJSON;
  }

  @Nullable
  public Map<String, Object> getReal() {
    return real;
  }

  public void setReal(@Nullable Map<String, Object> real) {
    this.real = real;
  }

  /* ------------ IBO Getters ------------ */

  @BsonIgnore
  public SensorTelemetryEventIBO toSensorTelemetryEventIBO() {
    try {
      return deserializeSensorTelemetryEventIBO(this.schemaJSON, this.recordJSON);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BsonIgnore
  public SensorTelemetryMeasurementEventIBO toSensorTelemetryMeasurementEventIBO() {
    try {
      return deserializeSensorTelemetryMeasurementEventIBO(this.schemaJSON, this.recordJSON);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BsonIgnore
  public SensorTelemetryMeasurementsAverageEventIBO toSensorTelemetryMeasurementsAverageEventIBO() {
    try {
      return deserializeSensorTelemetryMeasurementsAverageEventIBO(
          this.schemaJSON, this.recordJSON);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /* ------------ Instances ------------ */

  public static PersistedIBO fromIBO(SensorTelemetryRawEventIBO ibo) {
    final String recordJSON;
    try {
      recordJSON = serializeSensorTelemetryRawEventIBO(ibo);
    } catch (IOException ex) {
      LOGGER.error("Failed to serialize SensorTelemetryRawEventIBO", ex);
      throw new RuntimeException(ex);
    }

    final Map<String, Object> real;
    try {
      real = convertJSONToMap(recordJSON);
    } catch (JsonProcessingException ex) {
      LOGGER.error("Failed to convert SensorTelemetryRawEventIBO JSON to Map", ex);
      throw new RuntimeException(ex);
    }

    return new PersistedIBO(null, null, ibo.getSchema().toString(), recordJSON, real);
  }

  public static PersistedIBO fromIBO(SensorTelemetryEventIBO ibo) {
    final String recordJSON;
    try {
      recordJSON = serializeSensorTelemetryEventIBO(ibo);
    } catch (IOException ex) {
      LOGGER.error("Failed to serialize SensorTelemetryEventIBO", ex);
      throw new RuntimeException(ex);
    }

    final Map<String, Object> real;
    try {
      real = convertJSONToMap(recordJSON);
    } catch (JsonProcessingException ex) {
      LOGGER.error("Failed to convert SensorTelemetryEventIBO JSON to Map", ex);
      throw new RuntimeException(ex);
    }

    return new PersistedIBO(null, null, ibo.getSchema().toString(), recordJSON, real);
  }

  public static PersistedIBO fromIBO(SensorTelemetryMeasurementEventIBO ibo) {
    final String recordJSON;
    try {
      recordJSON = serializeSensorTelemetryMeasurementEventIBO(ibo);
    } catch (IOException ex) {
      LOGGER.error("Failed to serialize SensorTelemetryMeasurementEventIBO", ex);
      throw new RuntimeException(ex);
    }

    final Map<String, Object> real;
    try {
      real = convertJSONToMap(recordJSON);
    } catch (JsonProcessingException ex) {
      LOGGER.error("Failed to convert SensorTelemetryMeasurementEventIBO JSON to Map", ex);
      throw new RuntimeException(ex);
    }

    return new PersistedIBO(null, null, ibo.getSchema().toString(), recordJSON, real);
  }

  public static PersistedIBO fromIBO(SensorTelemetryMeasurementsAverageEventIBO ibo) {
    final String recordJSON;
    try {
      recordJSON = serializeSensorTelemetryMeasurementsAverageEventIBO(ibo);
    } catch (IOException ex) {
      LOGGER.error("Failed to serialize SensorTelemetryMeasurementsAverageEventIBO", ex);
      throw new RuntimeException(ex);
    }

    final Map<String, Object> real;
    try {
      real = convertJSONToMap(recordJSON);
    } catch (JsonProcessingException ex) {
      LOGGER.error("Failed to convert SensorTelemetryMeasurementsAverageEventIBO JSON to Map", ex);
      throw new RuntimeException(ex);
    }

    return new PersistedIBO(null, null, ibo.getSchema().toString(), recordJSON, real);
  }

  /* ------------ Internals ------------ */

  private static Map<String, Object> convertJSONToMap(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, TYPE_REFERENCE);
  }

  /* ------------ Internals: Ser/De: SensorTelemetryRawEventIBO ------------ */

  private static String serializeSensorTelemetryRawEventIBO(
      SensorTelemetryRawEventIBO specificRecord) throws IOException {
    final DatumWriter<SensorTelemetryRawEventIBO> writer =
        new SpecificDatumWriter<>(specificRecord.getSchema());
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder jsonEncoder =
        EncoderFactory.get().jsonEncoder(specificRecord.getSchema(), stream);
    writer.write(specificRecord, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString(StandardCharsets.UTF_8);
  }

  private static SensorTelemetryRawEventIBO deserializeSensorTelemetryRawEventIBO(
      String schemaJSON, String recordJSON) throws IOException {
    final Schema schema = new Schema.Parser().parse(schemaJSON);
    final DatumReader<SensorTelemetryRawEventIBO> reader = new SpecificDatumReader<>(schema);
    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, recordJSON);
    return reader.read(null, decoder);
  }

  /* ------------ Internals: Ser/De: SensorTelemetryEventIBO ------------ */

  private static String serializeSensorTelemetryEventIBO(SensorTelemetryEventIBO specificRecord)
      throws IOException {
    final DatumWriter<SensorTelemetryEventIBO> writer =
        new SpecificDatumWriter<>(specificRecord.getSchema());
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder jsonEncoder =
        EncoderFactory.get().jsonEncoder(specificRecord.getSchema(), stream);
    writer.write(specificRecord, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString(StandardCharsets.UTF_8);
  }

  private static SensorTelemetryEventIBO deserializeSensorTelemetryEventIBO(
      String schemaJSON, String recordJSON) throws IOException {
    final Schema schema = new Schema.Parser().parse(schemaJSON);
    final DatumReader<SensorTelemetryEventIBO> reader = new SpecificDatumReader<>(schema);
    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, recordJSON);
    return reader.read(null, decoder);
  }

  /* ------------ Internals: Ser/De: SensorTelemetryMeasurementEventIBO ------------ */

  private static String serializeSensorTelemetryMeasurementEventIBO(
      SensorTelemetryMeasurementEventIBO specificRecord) throws IOException {
    final DatumWriter<SensorTelemetryMeasurementEventIBO> writer =
        new SpecificDatumWriter<>(specificRecord.getSchema());
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder jsonEncoder =
        EncoderFactory.get().jsonEncoder(specificRecord.getSchema(), stream);
    writer.write(specificRecord, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString(StandardCharsets.UTF_8);
  }

  private static SensorTelemetryMeasurementEventIBO deserializeSensorTelemetryMeasurementEventIBO(
      String schemaJSON, String recordJSON) throws IOException {
    final Schema schema = new Schema.Parser().parse(schemaJSON);
    final DatumReader<SensorTelemetryMeasurementEventIBO> reader =
        new SpecificDatumReader<>(schema);
    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, recordJSON);
    return reader.read(null, decoder);
  }

  /* ------------ Internals: Ser/De: SensorTelemetryMeasurementsAverageEventIBO ------------ */

  private static String serializeSensorTelemetryMeasurementsAverageEventIBO(
      SensorTelemetryMeasurementsAverageEventIBO specificRecord) throws IOException {
    final DatumWriter<SensorTelemetryMeasurementsAverageEventIBO> writer =
        new SpecificDatumWriter<>(specificRecord.getSchema());
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder jsonEncoder =
        EncoderFactory.get().jsonEncoder(specificRecord.getSchema(), stream);
    writer.write(specificRecord, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString(StandardCharsets.UTF_8);
  }

  private static SensorTelemetryMeasurementsAverageEventIBO
      deserializeSensorTelemetryMeasurementsAverageEventIBO(String schemaJSON, String recordJSON)
          throws IOException {
    final Schema schema = new Schema.Parser().parse(schemaJSON);
    final DatumReader<SensorTelemetryMeasurementsAverageEventIBO> reader =
        new SpecificDatumReader<>(schema);
    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, recordJSON);
    return reader.read(null, decoder);
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return "PersistedIBO{" + "id=" + id + '}';
  }
}
