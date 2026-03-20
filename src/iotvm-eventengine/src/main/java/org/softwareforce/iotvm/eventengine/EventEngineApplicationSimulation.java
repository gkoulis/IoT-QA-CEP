package org.softwareforce.iotvm.eventengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.configuration.ApplicationConfiguration;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceType;
import org.softwareforce.iotvm.eventengine.simulation.Simulation;

public class EventEngineApplicationSimulation {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EventEngineApplicationSimulation.class);

  public static void main(String[] args) {
    new EventEngineApplicationSimulation().run();
  }

  public void run() {
    ApplicationConfiguration.getInstance().load();

    // TODO run it with assertions enabled.
    // TODO validate the new event fabrication service -> tests etc. IMPORTANT.

    long start = System.nanoTime();

    // TODO Arguments or env or both.
    // final String simulationsDirectoryPath = System.getenv("SIMULATIONS_DIRECTORY_PATH");
    // final String baseDirectory =
    // "/Users/gkoulis/projects/dgk-phd-monorepo/src/iotvm-local-data/simulations";
    final String baseDirectory =
        "/home/dgk/projects/PhD/dgk-phd-monorepo/src/iotvm-local-data/simulations";
    final String simulationName = "simulation-1";
    final IBOPersistenceServiceType iboPersistenceServiceType = IBOPersistenceServiceType.NO_OPS;

    final Simulation simulation =
        new Simulation(baseDirectory, simulationName, iboPersistenceServiceType);
    // TODO Run validations, diagnostics.

    simulation.execute();

    long end = System.nanoTime();
    long diff = end - start;
    LOGGER.info("EventEngineApplicationSimulation terminated after {} ns", diff);
  }
}
