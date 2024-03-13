package org.softwareforce.iotvm.eventengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.configuration.ApplicationConfiguration;
import org.softwareforce.iotvm.eventengine.simulation.Simulation;

public class EventEngineApplicationSimulation {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EventEngineApplicationSimulation.class);

  public static void main(String[] args) {
    new EventEngineApplicationSimulation().run();
  }

  public void run() {
    ApplicationConfiguration.getInstance().load();

    // TODO run it with enabled assertions.
    // TODO validate the new event fabrication service -> tests etc. IMPORTANT.

    // TODO Arguments or env or both.
    long start = System.nanoTime();
    final String baseDirectory =
        "/home/dgk/projects/PhD/dgk-phd-monorepo/src/iotvm-extensions/local_data/simulation1-EXAMPLE";
    final String simulationName = "simulation-1";

    final Simulation simulation = new Simulation(baseDirectory, simulationName);
    // TODO Run validations, diagnostics.

    simulation.execute();
    long end = System.nanoTime();
    long diff = end - start;
    System.out.println(diff); // TODO remove.
  }
}
