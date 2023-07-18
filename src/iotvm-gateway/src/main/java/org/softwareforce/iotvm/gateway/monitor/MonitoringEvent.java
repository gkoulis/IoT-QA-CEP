package org.softwareforce.iotvm.gateway.monitor;

import java.io.Serializable;
import java.util.Map;

/**
 * Monitoring Event POJO.
 *
 * @author Dimitris Gkoulis
 */
public class MonitoringEvent implements Serializable {

  private String topicName;
  private Map<String, Object> real;

  /* ------------ Constructors ------------ */

  public MonitoringEvent() {}

  public MonitoringEvent(String topicName, Map<String, Object> real) {
    this.topicName = topicName;
    this.real = real;
  }

  /* ------------ Getters and Setters ------------ */

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public Map<String, Object> getReal() {
    return real;
  }

  public void setReal(Map<String, Object> real) {
    this.real = real;
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return "MonitoringEvent{}";
  }
}
