package software.dgk.mozart.prototype1.usecase1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic aggregate box for Kafka streams {@code aggregate}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 9 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public class UC1GenericAggregateBox implements Serializable {

  private static final long serialVersionUID = 1L;

  public Map<String, Double> data;
  public Double mean;
  public Long timestamp;

  public UC1GenericAggregateBox() {
    this.data = new HashMap<>();
    this.mean = 0D;
    this.timestamp = 0L;
  }

  public void calculateMean() {
    int i = 0;
    this.mean = 0D;
    for (Map.Entry<String, Double> entry : this.data.entrySet()) {
      if (entry.getKey().startsWith("device_value_")) {
        this.mean = this.mean + entry.getValue();
        i++;
      }
    }
    if (i > 0) {
      this.mean = this.mean / i;
    } else {
      this.mean = 0D;
    }
  }

  @JsonIgnore
  public Instant timestampToInstant() {
    return Instant.ofEpochMilli(this.timestamp);
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("UC1GenericAggregateBox{");
    sb.append("data=").append(data);
    sb.append(", mean=").append(mean);
    sb.append(", timestamp=").append(this.timestampToInstant().toString());
    sb.append('}');
    return sb.toString();
  }
}
