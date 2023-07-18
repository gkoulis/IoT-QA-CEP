package org.softwareforce.iotvm.gateway.configuration;

import com.codahale.metrics.health.HealthCheck;

/**
 * Simple health check implementation for application.
 *
 * @author Dimitris Gkoulis
 */
public class ApplicationHealthCheck extends HealthCheck {

  @Override
  protected Result check() throws Exception {
    return Result.healthy();
  }
}
