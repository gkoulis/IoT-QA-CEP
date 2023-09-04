package org.softwareforce.iotvm.eventengine.cep.ct;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Tests for validating the functionality of {@link Duration}.
 *
 * @author Dimitris Gkoulis
 */
public class DurationTests {

  @Test
  public void test1() {
    Duration duration1 = Duration.ofMillis(10);
    System.out.println(duration1.toString());
    System.out.println(duration1.getSeconds());
    System.out.println(duration1.getNano());

    Duration duration2 = Duration.ofSeconds(100000000);
    System.out.println(duration2.toString());
    System.out.println(duration2.getSeconds());
    System.out.println(duration2.getNano());

    Duration duration3 = Duration.ofSeconds(10);
    System.out.println(duration3.toString());
    System.out.println(duration3.getSeconds());
    System.out.println(duration3.getNano());

    Duration duration4 = Duration.ofSeconds(60);
    System.out.println(duration4.toString());
    System.out.println(duration4.getSeconds());
    System.out.println(duration4.getNano());

    Duration duration5 = Duration.ofSeconds(100);
    System.out.println(duration5.toString());
    System.out.println(duration5.getSeconds());
    System.out.println(duration5.getNano());
  }
}
