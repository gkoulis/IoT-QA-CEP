package org.softwareforce.iotvm.eventengine.simulation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Simulation-Variation-Iteration object with information and data providers for execution.
 *
 * @author Dimitris Gkoulis
 * @createdAt Tuesday 12 March 2024
 */
public class SimulationVariationIteration {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimulationVariationIteration.class);

  private final String baseDirectory;
  private final String simulationName;
  private final String variationName;
  private final String iterationName;
  private final Path directory;

  public SimulationVariationIteration(
      String baseDirectory, String simulationName, String variationName, String iterationName) {
    this.baseDirectory = baseDirectory;
    this.simulationName = simulationName;
    this.variationName = variationName;
    this.iterationName = iterationName;
    this.directory =
        Paths.get(
            this.baseDirectory,
            this.simulationName,
            this.variationName,
            this.iterationName,
            "_system",
            "input");
  }

  public String getSimulationName() {
    return this.simulationName;
  }

  public String getVariationName() {
    return this.variationName;
  }

  public String getIterationName() {
    return this.iterationName;
  }

  public Path getDirectory() {
    return this.directory;
  }

  public List<SensorTelemetryRawEventIBO> loadSensorTelemetryRawEventIBOList() throws IOException {
    final String pathToFile = this.directory.resolve("SensorTelemetryRawEventIBO.json").toString();
    final File jsonFile = new File(pathToFile);
    final ObjectMapper objectMapper = new ObjectMapper();
    //noinspection Convert2Diamond
    final List<Object> jsonObjects =
        objectMapper.readValue(jsonFile, new TypeReference<List<Object>>() {});
    final List<SensorTelemetryRawEventIBO> sensorTelemetryRawEventIBOList = new ArrayList<>();
    for (final Object jsonObject : jsonObjects) {
      final String jsonString = objectMapper.writeValueAsString(jsonObject);
      final Decoder decoder =
          DecoderFactory.get().jsonDecoder(SensorTelemetryRawEventIBO.getClassSchema(), jsonString);
      final DatumReader<SensorTelemetryRawEventIBO> reader =
          new SpecificDatumReader<>(SensorTelemetryRawEventIBO.class);
      final SensorTelemetryRawEventIBO sensorTelemetryRawEventIBO = reader.read(null, decoder);
      sensorTelemetryRawEventIBOList.add(sensorTelemetryRawEventIBO);
    }
    return sensorTelemetryRawEventIBOList;
  }
}
