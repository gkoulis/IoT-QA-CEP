package org.softwareforce.iotvm.eventengine.cep.ct;

/**
 * Parameters for implementations of {@link CompositeTransformationFactory}.
 *
 * @author Dimitris Gkoulis
 */
public abstract class CompositeTransformationParameters {

  /**
   * @return a {@link String} representing the unique identifier of the provisioned composite
   *     transformation.
   */
  public abstract String getUniqueIdentifier();
}
