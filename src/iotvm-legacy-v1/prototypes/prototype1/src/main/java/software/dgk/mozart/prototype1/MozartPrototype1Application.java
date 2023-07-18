package software.dgk.mozart.prototype1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <strong>Mozart Prototype1</strong> Spring Boot application.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@SpringBootApplication
public class MozartPrototype1Application {

  private static final Logger log = LoggerFactory.getLogger(MozartPrototype1Application.class);

  public static void main(String[] args) {
    SpringApplication.run(MozartPrototype1Application.class, args);
  }
}
