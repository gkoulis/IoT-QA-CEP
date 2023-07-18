package org.softwareforce.iotvm.eventengine.cep.ct;

import java.time.Instant;
import java.util.List;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;

/**
 * Composite Transformation for merging progressively the outputs of {@link
 * AverageCalculationCompositeTransformationFactory}.
 *
 * @author Dimitris Gkoulis
 */
public class AverageCalculationMergingCompositeTransformationFactory
    extends CompositeTransformationFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AverageCalculationMergingCompositeTransformationFactory.class);
  private static final String NAME = "average_calculation_merging";

  private final AverageCalculationMergingCompositeTransformationParameters parameters;

  private final IBOPersistenceServiceImpl iboPersistenceService;

  /* ------------ Constructors ------------ */

  public AverageCalculationMergingCompositeTransformationFactory(
      AverageCalculationMergingCompositeTransformationParameters parameters,
      IBOPersistenceServiceImpl iboPersistenceService) {
    this.parameters = parameters;
    this.iboPersistenceService = iboPersistenceService;
  }

  /* ------------ Internals ------------ */

  private StreamsBuilder buildForMergeStage1(StreamsBuilder streamsBuilder) {

    final Consumed<String, SensorTelemetryMeasurementsAverageEventIBO> consumed =
        Consumed.with(
                Constants.STRING_SERDE,
                Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE)
            .withTimestampExtractor(new ValidNonNullTimestampExtractor());
    final Produced<String, SensorTelemetryMeasurementsAverageEventIBO> produced =
        Produced.with(
            Constants.STRING_SERDE,
            Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE);

    for (final AverageCalculationCompositeTransformationParameters parametersSet :
        this.parameters.getParameters()) {
      final String inputTopicName =
          Constants.getSensorTelemetryMeasurementsAverageEventTopic(
              parametersSet.getPhysicalQuantity(), parametersSet.getUniqueIdentifier());
      final String outputTopicName =
          Constants.getSensorTelemetryMeasurementsAverageEventTopic(
              parametersSet.getPhysicalQuantity());

      streamsBuilder.stream(inputTopicName, consumed)
          .mapValues(
              (value) -> {
                value
                    .getTimestamps()
                    .getTimestamps()
                    .put(Constants.MERGED_L1, Instant.now().toEpochMilli());
                return this.iboPersistenceService.saveAlt(outputTopicName, value);
              })
          .to(outputTopicName, produced);
    }

    return streamsBuilder;
  }

  private StreamsBuilder buildForMergeStage2(StreamsBuilder streamsBuilder) {
    final List<PhysicalQuantity> physicalQuantities =
        this.parameters.getParameters().stream()
            .map(AverageCalculationCompositeTransformationParameters::getPhysicalQuantity)
            .distinct()
            .toList();

    final Consumed<String, SensorTelemetryMeasurementsAverageEventIBO> consumed =
        Consumed.with(
                Constants.STRING_SERDE,
                Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE)
            .withTimestampExtractor(new ValidNonNullTimestampExtractor());
    final Produced<String, SensorTelemetryMeasurementsAverageEventIBO> produced =
        Produced.with(
            Constants.STRING_SERDE,
            Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE);

    for (final PhysicalQuantity physicalQuantity : physicalQuantities) {
      final String inputTopicName =
          Constants.getSensorTelemetryMeasurementsAverageEventTopic(physicalQuantity);
      final String outputTopicName = Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC;

      streamsBuilder.stream(inputTopicName, consumed)
          .mapValues(
              (value) -> {
                value
                    .getTimestamps()
                    .getTimestamps()
                    .put(Constants.MERGED_L2, Instant.now().toEpochMilli());
                return this.iboPersistenceService.saveAlt(outputTopicName, value);
              })
          .to(outputTopicName, produced);
    }

    return streamsBuilder;
  }

  /* ------------ Implementation ------------ */

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public CompositeTransformationParameters getParameters() {
    return this.parameters;
  }

  @SuppressWarnings("DataFlowIssue")
  @Override
  public StreamsBuilder build(StreamsBuilder streamsBuilder) {
    streamsBuilder = this.buildForMergeStage1(streamsBuilder);
    streamsBuilder = this.buildForMergeStage2(streamsBuilder);
    return streamsBuilder;
  }
}
