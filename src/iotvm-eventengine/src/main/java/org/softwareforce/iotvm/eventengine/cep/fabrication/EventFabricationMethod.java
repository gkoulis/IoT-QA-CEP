package org.softwareforce.iotvm.eventengine.cep.fabrication;

/**
 * Represents a supported event fabrication method.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public enum EventFabricationMethod {
  NAIVE,
  SIMPLE_EXPONENTIAL_SMOOTHING,
  EXPONENTIAL_SMOOTHING_WITH_LINEAR_TREND,
}
