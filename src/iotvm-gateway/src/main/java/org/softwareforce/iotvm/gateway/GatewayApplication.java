package org.softwareforce.iotvm.gateway;

import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterRegistration;
import java.util.EnumSet;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.softwareforce.iotvm.gateway.configuration.ApplicationHealthCheck;
import org.softwareforce.iotvm.gateway.configuration.BasicConfiguration;
import org.softwareforce.iotvm.gateway.controller.SensorTelemetryEventController;
import org.softwareforce.iotvm.gateway.kafka.KafkaProducerFactory;
import org.softwareforce.iotvm.gateway.monitor.MonitorResource;
import org.softwareforce.iotvm.gateway.service.SensorTelemetryEventService;

/**
 * Gateway Application.
 *
 * @author Dimitris Gkoulis
 */
public class GatewayApplication extends Application<BasicConfiguration> {

  public static void main(final String[] args) throws Exception {
    new GatewayApplication().run(args);
  }

  /* ------------ Internals ------------ */

  private void setCors(final Environment environment) {
    final FilterRegistration.Dynamic cors =
        environment.servlets().addFilter("CORS", CrossOriginFilter.class);

    // Configure CORS parameters
    cors.setInitParameter("allowedOrigins", "*");
    // cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
    cors.setInitParameter("allowedHeaders", "*");
    cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

    // Add URL mapping
    cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
  }

  /* ------------ Interface Implementation ------------ */

  @Override
  public String getName() {
    return "iotvm-gateway";
  }

  @Override
  public void run(final BasicConfiguration basicConfiguration, final Environment environment) {
    final ApplicationHealthCheck applicationHealthCheck = new ApplicationHealthCheck();

    final KafkaProducerFactory kafkaProducerFactory = new KafkaProducerFactory();
    final SensorTelemetryEventService sensorTelemetryEventService =
        new SensorTelemetryEventService(kafkaProducerFactory.getKafkaProducer());
    final SensorTelemetryEventController sensorTelemetryEventController =
        new SensorTelemetryEventController(sensorTelemetryEventService);
    final MonitorResource monitorResource = new MonitorResource();

    environment.healthChecks().register("application", applicationHealthCheck);
    environment.jersey().register(sensorTelemetryEventController);
    environment.jersey().register(monitorResource);

    this.setCors(environment);
  }

  @Override
  public void initialize(final Bootstrap<BasicConfiguration> bootstrap) {
    // bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false)));
    super.initialize(bootstrap);
  }
}
