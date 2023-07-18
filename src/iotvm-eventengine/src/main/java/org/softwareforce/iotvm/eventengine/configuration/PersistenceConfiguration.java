package org.softwareforce.iotvm.eventengine.configuration;

/**
 * Configuration for persistence.
 *
 * @author Dimitris Gkoulis
 */
public class PersistenceConfiguration {

  private String mongoUri;
  private String databaseName;

  /* ------------ Constructors ------------ */

  public PersistenceConfiguration() {}

  public PersistenceConfiguration(String mongoUri, String databaseName) {
    this.mongoUri = mongoUri;
    this.databaseName = databaseName;
  }

  /* ------------ Getters and Setters ------------ */

  public String getMongoUri() {
    return mongoUri;
  }

  public void setMongoUri(String mongoUri) {
    this.mongoUri = mongoUri;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }
}
