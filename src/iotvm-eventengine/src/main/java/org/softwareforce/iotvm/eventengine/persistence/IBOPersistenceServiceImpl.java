package org.softwareforce.iotvm.eventengine.persistence;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.InsertOneResult;

import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nullable;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.persistence.model.PersistedIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Service for managing persistence of IBO entities.
 *
 * <p>TODO make persistence optional.
 *
 * @author Dimitris Gkoulis
 */
public class IBOPersistenceServiceImpl {

  private static final String COLLECTION_NAME = "universal";

  /** The MongoDB client. */
  @SuppressWarnings("FieldCanBeLocal")
  private final MongoClient mongoClient;

  /** The MongoDB {@link org.bson.codecs.Codec} registry. */
  @SuppressWarnings("FieldCanBeLocal")
  private final CodecRegistry codecRegistry;

  /** The universal collection for storing IBO entities. */
  private final MongoCollection<PersistedIBO> universalCollection;

  /* ------------ Constructors ------------ */

  public IBOPersistenceServiceImpl(final PersistenceConfiguration persistenceConfiguration) {
    final ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(persistenceConfiguration.getMongoUri()))
            .serverApi(serverApi)
            .build();

    this.mongoClient = MongoClients.create(settings);

    final CodecProvider pojoCodecProvider =
        PojoCodecProvider.builder()
            .register("org.softwareforce.iotvm.eventengine.persistence.model")
            .build();
    this.codecRegistry =
        fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));

    final MongoDatabase database =
        this.mongoClient.getDatabase(persistenceConfiguration.getDatabaseName());
    this.universalCollection =
        database
            .getCollection(COLLECTION_NAME, PersistedIBO.class)
            .withCodecRegistry(this.codecRegistry);
  }

  /* ------------ Logic ------------ */

  public PersistedIBO save(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryRawEventIBO sensorTelemetryRawEventIBO) {
    final PersistedIBO persistedIBO = PersistedIBO.fromIBO(sensorTelemetryRawEventIBO);
    if (objectId == null) {
      objectId = ObjectId.get();
    }
    persistedIBO.setId(objectId);
    persistedIBO.setTopicName(topicName);
    final InsertOneResult insertOneResult = this.universalCollection.insertOne(persistedIBO);
    // persistedIBO.setId(insertOneResult.getInsertedId().asObjectId().getValue());
    return persistedIBO;
  }

  public PersistedIBO save(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryEventIBO sensorTelemetryEventIBO) {
    final PersistedIBO persistedIBO = PersistedIBO.fromIBO(sensorTelemetryEventIBO);
    if (objectId == null) {
      objectId = ObjectId.get();
    }
    persistedIBO.setId(objectId);
    persistedIBO.setTopicName(topicName);
    final InsertOneResult insertOneResult = this.universalCollection.insertOne(persistedIBO);
    // persistedIBO.setId(insertOneResult.getInsertedId().asObjectId().getValue());
    return persistedIBO;
  }

  public PersistedIBO save(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryMeasurementEventIBO sensorTelemetryMeasurementEventIBO) {
    final PersistedIBO persistedIBO = PersistedIBO.fromIBO(sensorTelemetryMeasurementEventIBO);
    if (objectId == null) {
      objectId = ObjectId.get();
    }
    persistedIBO.setId(objectId);
    persistedIBO.setTopicName(topicName);
    final InsertOneResult insertOneResult = this.universalCollection.insertOne(persistedIBO);
    // persistedIBO.setId(insertOneResult.getInsertedId().asObjectId().getValue());
    return persistedIBO;
  }

  public PersistedIBO save(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryMeasurementsAverageEventIBO sensorTelemetryMeasurementsAverageEventIBO) {
    final PersistedIBO persistedIBO =
        PersistedIBO.fromIBO(sensorTelemetryMeasurementsAverageEventIBO);
    if (objectId == null) {
      objectId = ObjectId.get();
    }
    persistedIBO.setId(objectId);
    persistedIBO.setTopicName(topicName);
    final InsertOneResult insertOneResult = this.universalCollection.insertOne(persistedIBO);
    // persistedIBO.setId(insertOneResult.getInsertedId().asObjectId().getValue());
    return persistedIBO;
  }

  /**
   * Finds the most recent {@link SensorTelemetryMeasurementEventIBO} record for a given time
   * period.
   *
   * <p>NOTICE: the {@code topicName} is version. This method will search records of the current
   * version of topic name as defined in {@link
   * Constants#getSensorTelemetryMeasurementEventTopic(PhysicalQuantity)} and {@link
   * Constants#SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC}.
   *
   * @param physicalQuantity the {@link PhysicalQuantity} based on which the topic is given.
   * @param sensorId the ID of the sensor that produced the {@link
   *     SensorTelemetryMeasurementEventIBO} instance.
   * @param startTimestamp the start timestamp in epoch millis.
   * @param endTimestamp the end timestamp in epoch millis.
   * @return an {@link Optional} with the {@link SensorTelemetryMeasurementEventIBO} instance or an
   *     empty {@link Optional} if there is no instance.
   */
  public Optional<SensorTelemetryMeasurementEventIBO> findPastSensorTelemetryMeasurementEventIBO(
      final PhysicalQuantity physicalQuantity,
      final String sensorId,
      final long startTimestamp,
      final long endTimestamp) {

    final String topicName = Constants.getSensorTelemetryMeasurementEventTopic(physicalQuantity);

    final Bson filter =
        Filters.and(
            Filters.eq("topicName", topicName),
            Filters.eq("real.sensorId", sensorId),
            Filters.gte("real.timestamps.defaultTimestamp.long", startTimestamp),
            Filters.lt("real.timestamps.defaultTimestamp.long", endTimestamp));

    PersistedIBO persistedIBO =
        this.universalCollection
            .find(filter)
            .sort(Sorts.descending("real.timestamps.defaultTimestamp.long"))
            .first();

    if (persistedIBO == null) {
      return Optional.empty();
    }

    final SensorTelemetryMeasurementEventIBO sensorTelemetryMeasurementEventIBO =
        persistedIBO.toSensorTelemetryMeasurementEventIBO();
    return Optional.of(sensorTelemetryMeasurementEventIBO);
  }

  /* ------------ Helpers ------------ */

  public SensorTelemetryRawEventIBO saveAlt(
      final String topicName, SensorTelemetryRawEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this.save(objectId, topicName, value);

    return value;
  }

  public SensorTelemetryEventIBO saveAlt(final String topicName, SensorTelemetryEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this.save(objectId, topicName, value);

    return value;
  }

  public SensorTelemetryMeasurementEventIBO saveAlt(
      final String topicName, SensorTelemetryMeasurementEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this.save(objectId, topicName, value);

    return value;
  }

  public SensorTelemetryMeasurementsAverageEventIBO saveAlt(
      final String topicName, SensorTelemetryMeasurementsAverageEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this.save(objectId, topicName, value);

    return value;
  }
}
