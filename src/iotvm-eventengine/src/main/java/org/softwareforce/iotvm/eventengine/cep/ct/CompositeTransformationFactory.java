package org.softwareforce.iotvm.eventengine.cep.ct;

import org.apache.kafka.streams.StreamsBuilder;

/**
 * Factory for creating and provisioning parametric composite transformations.
 *
 * @author Dimitris Gkoulis
 */
public abstract class CompositeTransformationFactory {

  /**
   * @return a {@link String} with the name of the composite transformation.
   */
  public abstract String getName();

  /**
   * @return the {@link CompositeTransformationParameters} instance with the composite
   *     transformation parameters.
   */
  public abstract CompositeTransformationParameters getParameters();

  /**
   * @param streamsBuilder the Kafka Streams {@link StreamsBuilder}.
   * @return the updated {@link StreamsBuilder} instance.
   */
  public abstract StreamsBuilder build(StreamsBuilder streamsBuilder);

  /**
   * @return a {@link String} with the composite unique identifier of the composite transformation/
   */
  public final String getUniqueIdentifier() {
    return getName() + "." + getParameters().getUniqueIdentifier();
  }
}
