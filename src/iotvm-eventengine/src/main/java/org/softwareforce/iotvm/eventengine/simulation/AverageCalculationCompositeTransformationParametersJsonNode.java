package org.softwareforce.iotvm.eventengine.simulation;

import java.time.Duration;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationParameters;

public class AverageCalculationCompositeTransformationParametersJsonNode {
  public String physicalQuantity;
  public int timeWindowSize;
  public Integer timeWindowGrace;
  public Integer timeWindowAdvance;
  public int minimumNumberOfContributingSensors;
  public boolean ignoreCompletenessFiltering;
  public int pastWindowsLookup;
  public Integer forecastingWindowSize;
  public int futureWindowsLookup;

  public AverageCalculationCompositeTransformationParameters toParameters() {
    return new AverageCalculationCompositeTransformationParameters(
        PhysicalQuantity.valueOf(this.physicalQuantity),
        Duration.ofSeconds(this.timeWindowSize),
        this.timeWindowGrace == null ? null : Duration.ofSeconds(this.timeWindowGrace),
        this.timeWindowAdvance == null ? null : Duration.ofSeconds(this.timeWindowAdvance),
        this.minimumNumberOfContributingSensors,
        this.ignoreCompletenessFiltering,
        this.pastWindowsLookup,
        this.forecastingWindowSize == null ? null : Duration.ofSeconds(this.forecastingWindowSize),
        this.futureWindowsLookup);
  }
}
