package software.dgk.mozart.prototype1.usecase1;

import org.springframework.stereotype.Component;

/**
 * Factory for creating {@link UC1DeviceGenericEvent} instances.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Component
public class UC1DeviceGenericEventFactory {

  public UC1DeviceGenericEventFactory() {}

  public UC1DeviceGenericEvent createTemperature(UC1DeviceSpecificEvent specificEvent) {
    return new UC1DeviceGenericEvent(
        specificEvent.getDeviceId(),
        "TEMPERATURE",
        specificEvent.getTemperature(),
        specificEvent.getTemperatureUnit(),
        specificEvent.getTimestamp());
  }

  public UC1DeviceGenericEvent createMoisture(UC1DeviceSpecificEvent specificEvent) {
    return new UC1DeviceGenericEvent(
        specificEvent.getDeviceId(),
        "MOISTURE",
        specificEvent.getMoisture(),
        specificEvent.getMoistureUnit(),
        specificEvent.getTimestamp());
  }
}
