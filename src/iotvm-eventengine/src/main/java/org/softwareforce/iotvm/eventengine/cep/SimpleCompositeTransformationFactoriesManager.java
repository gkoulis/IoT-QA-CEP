package org.softwareforce.iotvm.eventengine.cep;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.CompositeTransformationFactory;

/**
 * Manager for conveniently collect and provision all composite transformations.
 *
 * @author Dimitris Gkoulis
 */
public final class SimpleCompositeTransformationFactoriesManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SimpleCompositeTransformationFactoriesManager.class);

  private boolean built;
  private final List<CompositeTransformationFactory> compositeTransformationFactories;

  /* ------------ Constructors ------------ */

  private SimpleCompositeTransformationFactoriesManager() {
    this.built = false;
    this.compositeTransformationFactories = new ArrayList<>();
  }

  /* ------------ Builder ------------ */

  public static SimpleCompositeTransformationFactoriesManager newInstance() {
    return new SimpleCompositeTransformationFactoriesManager();
  }

  public SimpleCompositeTransformationFactoriesManager withCompositeTransformationFactory(
      CompositeTransformationFactory compositeTransformationFactory) {
    LOGGER.info("Registering {}", compositeTransformationFactory);

    if (this.built) {
      throw new IllegalStateException(
          "CEP manager has already built the composite transformations!");
    }

    if (this.compositeTransformationFactories.stream()
        .anyMatch(
            i ->
                i.getUniqueIdentifier()
                    .equals(compositeTransformationFactory.getUniqueIdentifier()))) {
      throw new IllegalStateException(
          "CEP manager has already included the composite transformation "
              + compositeTransformationFactory.getUniqueIdentifier());
    }

    this.compositeTransformationFactories.add(compositeTransformationFactory);
    return this;
  }

  /* ------------ Logic ------------ */

  public StreamsBuilder build() {
    this.built = true;
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    for (final CompositeTransformationFactory factory : this.compositeTransformationFactories) {
      streamsBuilder = factory.build(streamsBuilder);
    }
    return streamsBuilder;
  }
}
