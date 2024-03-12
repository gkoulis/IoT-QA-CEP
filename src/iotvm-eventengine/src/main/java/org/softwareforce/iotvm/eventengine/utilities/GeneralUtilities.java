package org.softwareforce.iotvm.eventengine.utilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dimitris Gkoulis
 * @createdAt Tuesday 12 March 2024
 */
public final class GeneralUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneralUtilities.class);

  private GeneralUtilities() {}

  public static boolean deleteDirectorySafely(Path directory) {
    if (!Files.exists(directory)) {
      LOGGER.error("Failed to delete directory: {} (path does not exist)", directory);
      return false;
    }
    if (!Files.isDirectory(directory)) {
      LOGGER.error("Failed to delete directory: {} (path is not a directory)", directory);
      return false;
    }
    try {
      Files.walkFileTree(
          directory,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              Files.delete(dir);
              return FileVisitResult.CONTINUE;
            }
          });
      return true;
    } catch (IOException ex) {
      LOGGER.error("Failed to delete directory: {}", directory, ex);
      return false;
    }
  }

  public static <T> Optional<List<T>> loadListOfInstancesFromJSON(
      String pathToFile, Class<T> clazz) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      final List<T> list =
          objectMapper.readValue(
              new File(pathToFile),
              objectMapper.getTypeFactory().constructCollectionType(List.class, clazz));
      return Optional.of(list);
    } catch (Exception ex) {
      LOGGER.error("Failed to load list of instances of class : {}", clazz, ex);
      return Optional.empty();
    }
  }

  /**
   * Serializes any Avro SpecificRecord to a JSON string.
   *
   * @param <T> the type parameter for Avro SpecificRecord
   * @param specificRecord the Avro SpecificRecord to be serialized
   * @return the JSON string representation of the Avro SpecificRecord
   * @throws IOException if there is an error during serialization
   */
  public static <T extends SpecificRecord> String serializeInstanceOfSpecificRecordToJSON(
      T specificRecord) throws IOException {
    final DatumWriter<T> writer = new SpecificDatumWriter<>(specificRecord.getSchema());
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final Encoder jsonEncoder =
        EncoderFactory.get().jsonEncoder(specificRecord.getSchema(), stream);
    writer.write(specificRecord, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString(StandardCharsets.UTF_8);
  }

  public static <T extends SpecificRecord> void writeInstancesOfSpecificRecordAsJson(
      List<T> records, String pathToFile) throws IOException {
    final List<String> serializedRecords = new ArrayList<>();
    for (final T record : records) {
      serializedRecords.add(serializeInstanceOfSpecificRecordToJSON(record));
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(pathToFile, false))) {
      final StringBuilder sb = new StringBuilder();
      sb.append("[");
      sb.append("\n");
      for (int i = 0; i < serializedRecords.size(); i++) {
        sb.append("\t");
        sb.append(serializedRecords.get(i));
        if (i < serializedRecords.size() - 1) {
          sb.append(",");
        }
        sb.append("\n");
      }
      sb.append("]");
      writer.write(sb.toString());
    }
  }
}
