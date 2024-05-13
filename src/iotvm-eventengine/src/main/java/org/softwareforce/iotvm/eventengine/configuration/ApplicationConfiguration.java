package org.softwareforce.iotvm.eventengine.configuration;

/**
 * Application Configuration.
 *
 * @author Dimitris Gkoulis
 */
public final class ApplicationConfiguration {

  private static final ApplicationConfiguration INSTANCE = new ApplicationConfiguration();

  /* ------------ Config: IoTVM Extensions ------------ */

  // private String yourConfig = "";

  /* ------------ Constructors ------------ */

  private ApplicationConfiguration() {}

  public static ApplicationConfiguration getInstance() {
    return INSTANCE;
  }

  public void load() {
    // TODO Load from file (only once).
    // this.yourConfig = "value";
  }

  /* ------------ Getters ------------ */
}
