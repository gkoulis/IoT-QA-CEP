package software.dgk.mozart.prototype1.commandlinerunner;

import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Bootstrap tasks runner.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Component
@Order(100)
public class BootstrapTaskRunner implements CommandLineRunner {

  public BootstrapTaskRunner() {}

  @Override
  public void run(String... args) throws Exception {
    // System.out.println("Hello World!");
  }
}
