package org.softwareforce.iotvm.eventengine.configuration;

/**
 * Application Configuration.
 *
 * @author Dimitris Gkoulis
 */
public final class ApplicationConfiguration {

  private static final ApplicationConfiguration INSTANCE = new ApplicationConfiguration();

  /* ------------ Config: IoTVM Extensions ------------ */

  private String extensionsHost;
  private int extensionsPort;

  /* ------------ Constructors ------------ */

  private ApplicationConfiguration() {}

  public static ApplicationConfiguration getInstance() {
    return INSTANCE;
  }

  public void load() {
    // TODO Load from file (allow calling this method only once.).
    this.extensionsHost = "localhost";
    this.extensionsPort = 9003;
  }

  /* ------------ Getters ------------ */

  public String getExtensionsHost() {
    return extensionsHost;
  }

  public int getExtensionsPort() {
    return extensionsPort;
  }
}
