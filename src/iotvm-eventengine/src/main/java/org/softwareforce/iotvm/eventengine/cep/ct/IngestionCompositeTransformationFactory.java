package org.softwareforce.iotvm.eventengine.cep.ct;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FlexibleTimestampExtractor;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.IngestionProcessor;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Composite Transformation for ingestion.
 *
 * @author Dimitris Gkoulis
 */
public final class IngestionCompositeTransformationFactory extends CompositeTransformationFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IngestionCompositeTransformationFactory.class);
  private static final String NAME = "ingestion";

  private final IngestionCompositeTransformationParameters parameters;
  private final IBOPersistenceServiceImpl iboPersistenceService;

  /* ------------ Constructors ------------ */

  public IngestionCompositeTransformationFactory(
      IngestionCompositeTransformationParameters parameters,
      IBOPersistenceServiceImpl iboPersistenceService) {
    this.parameters = parameters;
    this.iboPersistenceService = iboPersistenceService;
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

  @Override
  public StreamsBuilder build(StreamsBuilder streamsBuilder) {
    LOGGER.debug(
        "Request to build {} {} composite transformation",
        this.getUniqueIdentifier(),
        this.getName());

    final String inputTopicName = Constants.SENSOR_TELEMETRY_RAW_EVENT_TOPIC;
    final Consumed<String, SensorTelemetryRawEventIBO> consumedWith =
        Consumed.with(Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE)
            .withTimestampExtractor(new FlexibleTimestampExtractor());

    final String outputTopicName = Constants.SENSOR_TELEMETRY_EVENT_TOPIC;
    final Produced<String, SensorTelemetryEventIBO> producedWith =
        Produced.with(Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_EVENT_IBO_SERDE);

    // final IngestionProcessor ingestionProcessor =
    //     new IngestionProcessor(this.iboPersistenceService, inputTopicName, outputTopicName);

    streamsBuilder.stream(inputTopicName, consumedWith)
        // @future Performance Warning!
        .process(
            () ->
                new IngestionProcessor(this.iboPersistenceService, inputTopicName, outputTopicName))
        .to(outputTopicName, producedWith);

    return streamsBuilder;
  }
}
