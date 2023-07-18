package org.softwareforce.iotvm.eventengine.cep.ct;

import java.time.Duration;
import javax.annotation.Nullable;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;

/**
 * Parameters for {@link AverageCalculationCompositeTransformationFactory}.
 *
 * @author Dimitris Gkoulis
 */
public class AverageCalculationCompositeTransformationParameters
    extends CompositeTransformationParameters {

  /** The {@link PhysicalQuantity} to calculate average. */
  private final PhysicalQuantity physicalQuantity;

  /** The duration of the time window. */
  private final Duration timeWindowSize;

  /** The grace period of the time window. */
  @Nullable private final Duration timeWindowGrace;

  /**
   * The advance duration of the time window. If it's {@code null} a normal time window will be
   * created, otherwise a hopping time window will be created.
   */
  @Nullable private final Duration timeWindowAdvance;

  /**
   * The minimum allowed number of contributing sensors to the calculation of the average value in
   * the specified time window.
   */
  private final int minimumNumberOfContributingSensors;

  /**
   * If it's {@code true}, the completeness filtering, i.e., remove the composite events that have
   * completeness value less than one, is ignored.
   */
  private final boolean ignoreCompletenessFiltering;

  /**
   * The number of past windows to lookup for instances to fill the missing events. If it's zero,
   * past windows lookup functionality is disabled.
   */
  private final int pastWindowsLookup;

  /* ------------ Constructors ------------ */

  public AverageCalculationCompositeTransformationParameters(
      PhysicalQuantity physicalQuantity,
      Duration timeWindowSize,
      @Nullable Duration timeWindowGrace,
      @Nullable Duration timeWindowAdvance,
      int minimumNumberOfContributingSensors,
      boolean ignoreCompletenessFiltering,
      int pastWindowsLookup) {
    this.physicalQuantity = physicalQuantity;
    this.timeWindowSize = timeWindowSize;
    this.timeWindowGrace = timeWindowGrace;
    this.timeWindowAdvance = timeWindowAdvance;
    this.minimumNumberOfContributingSensors = minimumNumberOfContributingSensors;
    this.ignoreCompletenessFiltering = ignoreCompletenessFiltering;
    this.pastWindowsLookup = pastWindowsLookup;

    if (this.timeWindowSize.equals(Duration.ZERO)) {
      throw new IllegalArgumentException("timeWindowSize cannot be ZERO. Use null instead!");
    }
    if (this.timeWindowSize.isNegative()) {
      throw new IllegalArgumentException("timeWindowSize cannot be negative. Use null instead!");
    }

    if (this.timeWindowGrace != null) {
      if (this.timeWindowGrace.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("timeWindowGrace cannot be ZERO. Use null instead!");
      }
      if (this.timeWindowGrace.isNegative()) {
        throw new IllegalArgumentException("timeWindowGrace cannot be negative. Use null instead!");
      }
    }

    if (this.timeWindowAdvance != null) {
      if (this.timeWindowAdvance.equals(Duration.ZERO)) {
        throw new IllegalArgumentException("timeWindowAdvance cannot be ZERO. Use null instead!");
      }
      if (this.timeWindowAdvance.isNegative()) {
        throw new IllegalArgumentException(
            "timeWindowAdvance cannot be negative. Use null instead!");
      }
    }

    if (this.pastWindowsLookup < 0) {
      throw new IllegalArgumentException("pastWindowsLookup must be zero or positive integer!");
    }
  }

  /* ------------ Getters ------------ */

  public PhysicalQuantity getPhysicalQuantity() {
    return physicalQuantity;
  }

  public Duration getTimeWindowSize() {
    return timeWindowSize;
  }

  @Nullable
  public Duration getTimeWindowGrace() {
    return timeWindowGrace;
  }

  @Nullable
  public Duration getTimeWindowAdvance() {
    return timeWindowAdvance;
  }

  public int getMinimumNumberOfContributingSensors() {
    return minimumNumberOfContributingSensors;
  }

  public boolean isIgnoreCompletenessFiltering() {
    return ignoreCompletenessFiltering;
  }

  public int getPastWindowsLookup() {
    return pastWindowsLookup;
  }

  /* ------------ Implementation ------------ */

  @Override
  public String getUniqueIdentifier() {
    //noinspection StringBufferReplaceableByString
    final StringBuilder sb = new StringBuilder("w_avg_");

    sb.append(this.physicalQuantity.getName());
    sb.append("_");

    sb.append(this.timeWindowSize.toString());
    sb.append("_");

    sb.append(this.timeWindowGrace == null ? "null" : this.timeWindowGrace.toString());
    sb.append("_");

    sb.append(this.timeWindowAdvance == null ? "null" : this.timeWindowAdvance.toString());
    sb.append("_");

    sb.append(this.minimumNumberOfContributingSensors);
    sb.append("_");

    sb.append(this.ignoreCompletenessFiltering);
    sb.append("_");

    sb.append(this.pastWindowsLookup);

    return sb.toString();
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return "AverageCalculationCompositeTransformationParameters{"
        + "physicalQuantity="
        + physicalQuantity
        + ", timeWindowSize="
        + timeWindowSize
        + ", timeWindowGrace="
        + timeWindowGrace
        + ", timeWindowAdvance="
        + timeWindowAdvance
        + ", minimumNumberOfContributingSensors="
        + minimumNumberOfContributingSensors
        + ", ignoreCompletenessFiltering="
        + ignoreCompletenessFiltering
        + ", pastWindowsLookup="
        + pastWindowsLookup
        + '}';
  }
}
