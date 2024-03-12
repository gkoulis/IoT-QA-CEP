package org.softwareforce.iotvm.eventengine.utilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Optional;

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
        if (Files.isDirectory(directory)) {
            LOGGER.error("Failed to delete directory: {} (path is not a directory)", directory);
            return false;
        }
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
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

    public static <T> Optional<List<T>> loadListOfInstancesFromJSON(String pathToFile, Class<T> clazz) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            final List<T> list = objectMapper.readValue(new File(pathToFile), objectMapper.getTypeFactory().constructCollectionType(List.class, clazz));
            return Optional.of(list);
        } catch (Exception ex) {
            LOGGER.error("Failed to load list of instances of class : {}", clazz, ex);
            return Optional.empty();
        }
    }
}
