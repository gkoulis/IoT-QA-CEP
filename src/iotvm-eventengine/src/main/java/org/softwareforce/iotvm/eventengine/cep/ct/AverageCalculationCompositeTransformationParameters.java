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
  public static final String ID_PREFIX = "w_avg_";

  // TODO as abstract method in abstract class!
  //  It must be unique across all to ensure uniqueness of ids.
  //  Check on start-up / registration of CTF, CT PS
  //  TODO create consistent language: CTF, CT, CT_PS, etc.

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
   * past windows lookup functionality is disabled. That is, backcasting fabrication is disabled.
   */
  private final int pastWindowsLookup;

  /**
   * The duration of the forecasting time series time window.
   *
   * <p>More specifically, this is the resolution of time series used to train the forecasting
   * model. It tells the time step (hourly, 15-minute, daily, etc.) Usually, it referred as
   * frequency (1m, 1h, 10s, etc.) The minimum logic value allowed is one second, i.e. {@code "1S"}.
   */
  private final Duration forecastingWindowSize;

  /**
   * The number of future windows to lookup for instances to fill the missing events. If it's zero,
   * future windows lookup functionality is disabled. That is, forecasting fabrication is disabled.
   */
  private final int futureWindowsLookup;

  /* ------------ Constructors ------------ */

  public AverageCalculationCompositeTransformationParameters(
      PhysicalQuantity physicalQuantity,
      Duration timeWindowSize,
      @Nullable Duration timeWindowGrace,
      @Nullable Duration timeWindowAdvance,
      int minimumNumberOfContributingSensors,
      boolean ignoreCompletenessFiltering,
      int pastWindowsLookup,
      Duration forecastingWindowSize,
      int futureWindowsLookup) {
    this.physicalQuantity = physicalQuantity;
    this.timeWindowSize = timeWindowSize;
    this.timeWindowGrace = timeWindowGrace;
    this.timeWindowAdvance = timeWindowAdvance;
    this.minimumNumberOfContributingSensors = minimumNumberOfContributingSensors;
    this.ignoreCompletenessFiltering = ignoreCompletenessFiltering;
    this.pastWindowsLookup = pastWindowsLookup;
    if (forecastingWindowSize == null) {
      this.forecastingWindowSize = this.timeWindowSize;
    } else {
      this.forecastingWindowSize = forecastingWindowSize;
    }
    this.futureWindowsLookup = futureWindowsLookup;

    requireValidDuration("timeWindowSize", this.timeWindowSize, false);
    requireValidDuration("timeWindowGrace", this.timeWindowGrace, true);
    requireValidDuration("timeWindowAdvance", this.timeWindowAdvance, true);

    if (this.forecastingWindowSize.compareTo(this.timeWindowSize) < 0) {
      throw new IllegalArgumentException(
          "forecastingWindowSize must be greater than or equal to timeWindowSize!");
    }

    if (this.pastWindowsLookup < 0) {
      throw new IllegalArgumentException("pastWindowsLookup must be zero or positive integer!");
    }

    requireValidDuration("forecastingWindowSize", this.forecastingWindowSize, false);

    if (this.futureWindowsLookup < 0) {
      throw new IllegalArgumentException("futureWindowsLookup must be zero or positive integer!");
    }
  }

  /* ------------ Validation Utils ------------ */

  private static void requireValidDuration(
      final String name, final Duration duration, final boolean nullable) {
    if (nullable && duration == null) {
      return;
    }
    if (duration.equals(Duration.ZERO)) {
      throw new IllegalArgumentException("Duration `" + name + "` cannot be ZERO!");
    }
    if (duration.isNegative()) {
      throw new IllegalArgumentException("Duration `" + name + "` cannot be negative!");
    }
    if (duration.getSeconds() < 1) {
      throw new IllegalArgumentException(
          "Duration `" + name + "` seconds must be greater than or equal to 1!");
    }
    if (duration.getNano() != 0) {
      throw new IllegalArgumentException("Duration `" + name + "` nano must be zero!");
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

  public Duration getForecastingWindowSize() {
    return forecastingWindowSize;
  }

  // TODO Temporarily enabled.
  // TODO Not sure!
  public int getFutureWindowsLookup() {
    return futureWindowsLookup;
  }

  /** TODO EXPERIMENTAL. */
  public int getFutureWindowsLookupAlternative() {
    if (this.futureWindowsLookup > 0 && this.pastWindowsLookup > 0) {
      return this.futureWindowsLookup + this.pastWindowsLookup;
    }
    return this.futureWindowsLookup;
  }

  /* ------------ Implementation ------------ */

  @Override
  public String getUniqueIdentifier() {
    //noinspection StringBufferReplaceableByString
    final StringBuilder sb = new StringBuilder(ID_PREFIX);

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
    sb.append("_");

    sb.append(this.forecastingWindowSize.toString());
    sb.append("_");

    sb.append(this.futureWindowsLookup);

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
        + ", forecastingWindowSize="
        + forecastingWindowSize
        + ", futureWindowsLookup="
        + futureWindowsLookup
        + '}';
  }
}
