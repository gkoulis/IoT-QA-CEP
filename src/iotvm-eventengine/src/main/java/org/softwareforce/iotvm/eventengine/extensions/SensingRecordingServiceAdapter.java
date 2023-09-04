package org.softwareforce.iotvm.eventengine.extensions;

import java.util.List;
import java.util.Optional;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.ForecastException;
import org.softwareforce.iotvm.shared.extensions.sensing_recording.spec.PhysicalQuantityDataPoint;
import org.softwareforce.iotvm.shared.extensions.sensing_recording.spec.SensingRecordingService;

/**
 * Adapter for connecting application with {@link SensingRecordingService}.
 *
 * @author Dimitris Gkoulis
 */
public class SensingRecordingServiceAdapter {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SensingRecordingServiceAdapter.class);

  /* ------------ Constructors ------------ */

  public SensingRecordingServiceAdapter() {}

  /* ------------ Internals ------------ */

  /* ------------ Logic ------------ */

  // TODO Temporary (better name, better objects, etc,).
  public Optional<Double> getRealAverage(
      List<String> sensorIds,
      PhysicalQuantity physicalQuantity,
      long fromTimestamp,
      long toTimestamp) {
    try {
      final SensingRecordingService.Client client =
          ExtensionsClientsFactory.getInstance().getSensingRecordingServiceClient().orElse(null);
      if (client == null) {
        return Optional.empty();
      }
      // TODO Prefer object (request param)!
      final List<PhysicalQuantityDataPoint> list =
          client.getBasicAggregationsCTFReals(
              sensorIds, physicalQuantity.getName(), fromTimestamp, toTimestamp);
      if (list.isEmpty()) {
        return Optional.empty();
      }
      // TODO Αν τα ID δεν είναι όλα μέσα;;;;
      return Optional.of(list.stream().map(i -> i.value).reduce(0D, Double::sum) / list.size());
    } catch (ForecastException ex) {
      LOGGER.warn(
          "getBasicAggregationsCTFReals ({}, {}, {}, {}) failed. Reason: {}",
          sensorIds,
          physicalQuantity.getName(),
          fromTimestamp,
          toTimestamp,
          ex.getReason());
      return Optional.empty();
    } catch (TException ex) {
      LOGGER.error(
          "getBasicAggregationsCTFReals ({}, {}, {}, {}) failed.",
          sensorIds,
          physicalQuantity.getName(),
          fromTimestamp,
          toTimestamp,
          ex);
      return Optional.empty();
    }
  }
}
