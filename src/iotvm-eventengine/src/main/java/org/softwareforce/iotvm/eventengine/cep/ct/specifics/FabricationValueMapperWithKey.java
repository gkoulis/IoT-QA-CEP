package org.softwareforce.iotvm.eventengine.cep.ct.specifics;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;

/**
 * Contract for processing a {@link SensorTelemetryMeasurementsAverageEventIBO} in order to
 * fabricate missing events.
 *
 * @author Dimitris Gkoulis
 */
public abstract class FabricationValueMapperWithKey
    implements ValueMapperWithKey<
        Windowed<String>,
        SensorTelemetryMeasurementsAverageEventIBO,
        SensorTelemetryMeasurementsAverageEventIBO> {

  /* ------------ Abstract Methods ------------ */

  /**
   * @return a {@link String} representing the name of the fabrication method. It is highly
   *     recommend to provide a camel-case formatted name.
   */
  public abstract String name();

  /**
   * Evaluates if fabrication must be applied.
   *
   * @param readOnlyKey the key.
   * @param value the value.
   * @return {@code true} if fabrication must be applied, otherwise {@code false}.
   */
  public abstract boolean shouldApply(
      Windowed<String> readOnlyKey, SensorTelemetryMeasurementsAverageEventIBO value);

  public abstract SensorTelemetryMeasurementsAverageEventIBO applyReal(
      Windowed<String> readOnlyKey, SensorTelemetryMeasurementsAverageEventIBO value);

  /* ------------ Interface Implementation ------------ */

  /**
   * Implementation of {@link ValueMapperWithKey#apply(Object, Object)} that wraps the {@link
   * #applyReal(Windowed, SensorTelemetryMeasurementsAverageEventIBO)} which implements the actual
   * business logic.
   *
   * @param readOnlyKey the read-only key
   * @param value the value to be mapped
   * @return the mapped value.
   */
  @Override
  public final SensorTelemetryMeasurementsAverageEventIBO apply(
      Windowed<String> readOnlyKey, SensorTelemetryMeasurementsAverageEventIBO value) {
    if (!value.getAdditional().containsKey("softSensingActivated")) {
      value.getAdditional().put("softSensingActivated", false);
    }
    value.getAdditional().put(this.name() + "Duration", 0L);
    value.getAdditional().put(this.name() + "Activated", false);
    value.getAdditional().put(this.name() + "Count", 0);
    // TODO add more? Yes.

    // Store current quality properties values before modifying them.
    value
        .getAdditional()
        .put(this.name() + "CompletenessBefore", value.getQualityProperties().getCompleteness());
    value
        .getAdditional()
        .put(this.name() + "TimelinessBefore", value.getQualityProperties().getTimeliness());

    if (!this.shouldApply(readOnlyKey, value)) {
      return value;
    }

    final long startNano = System.nanoTime();

    value = this.applyReal(readOnlyKey, value);

    final long endNano = System.nanoTime();
    final long nanoDiff = endNano - startNano;

    value.getAdditional().put("softSensingActivated", true);
    value.getAdditional().put(this.name() + "Duration", nanoDiff);
    value.getAdditional().put(this.name() + "Activated", true);

    return value;
  }
}
