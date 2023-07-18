package org.softwareforce.iotvm.eventengine.cep.ct;

import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composite Transformation for debugging.
 *
 * @author Dimitris Gkoulis
 */
public class DebuggingCompositeTransformationFactory extends CompositeTransformationFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DebuggingCompositeTransformationFactory.class);
  private static final String NAME = "debugging";

  private final DebuggingCompositeTransformationParameters parameters;

  /* ------------ Constructors ------------ */

  public DebuggingCompositeTransformationFactory(
      DebuggingCompositeTransformationParameters parameters) {
    this.parameters = parameters;
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
    return streamsBuilder;
  }
}
