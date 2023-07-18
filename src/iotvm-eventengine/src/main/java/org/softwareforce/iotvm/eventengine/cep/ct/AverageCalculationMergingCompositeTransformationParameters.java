package org.softwareforce.iotvm.eventengine.cep.ct;

import java.util.List;

/**
 * Parameters for {@link AverageCalculationMergingCompositeTransformationFactory}.
 *
 * @author Dimitris Gkoulis
 */
public class AverageCalculationMergingCompositeTransformationParameters
    extends CompositeTransformationParameters {

  private final List<AverageCalculationCompositeTransformationParameters> parameters;

  /* ------------ Constructors ------------ */

  public AverageCalculationMergingCompositeTransformationParameters(
      List<AverageCalculationCompositeTransformationParameters> parameters) {
    this.parameters = parameters;
  }

  /* ------------ Getters ------------ */

  public List<AverageCalculationCompositeTransformationParameters> getParameters() {
    // @future Make this immutable.
    return parameters;
  }

  /* ------------ Implementation ------------ */

  @Override
  public String getUniqueIdentifier() {
    return null;
  }
}
