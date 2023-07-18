package org.softwareforce.iotvm.eventengine.cep;

/**
 * Represents a supported physical quantity.
 *
 * @author Dimitris Gkoulis
 */
public enum PhysicalQuantity {
  TEMPERATURE("temperature", "CELSIUS"),
  HUMIDITY("humidity", "PERCENTAGE");

  private final String name;
  private final String unit;

  /* ------------ Constructors ------------ */

  PhysicalQuantity(String name, String unit) {
    this.name = name;
    this.unit = unit;
  }

  /* ------------ Getters ------------ */

  public String getName() {
    return this.name;
  }

  public String getUnit() {
    return unit;
  }
}
