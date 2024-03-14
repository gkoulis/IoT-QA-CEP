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
import com.mongodb.client.result.InsertOneResult;
import javax.annotation.Nullable;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.persistence.model.PersistedIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * MongoDB implementation of {@link IBOPersistenceService} by extending {@link
 * IBOPersistenceServiceBaseImpl}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 14 March 2024
 */
public class IBOPersistenceServiceMongoImpl extends IBOPersistenceServiceBaseImpl {

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

  public IBOPersistenceServiceMongoImpl(final PersistenceConfiguration persistenceConfiguration) {
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

  /* ------------ Private ------------ */

  private PersistedIBO _insert(
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

  private PersistedIBO _insert(
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

  private PersistedIBO _insert(
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

  private PersistedIBO _insert(
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
}
