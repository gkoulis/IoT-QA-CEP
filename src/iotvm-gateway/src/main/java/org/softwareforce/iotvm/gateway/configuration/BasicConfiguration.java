package org.softwareforce.iotvm.gateway.configuration;

import io.dropwizard.core.Configuration;
import jakarta.validation.constraints.NotEmpty;

/**
 * Simple configuration implementation for application.
 *
 * @author Dimitris Gkoulis
 */
public class BasicConfiguration extends Configuration {

  @NotEmpty private String comment;

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }
}
