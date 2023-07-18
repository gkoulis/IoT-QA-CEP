package org.softwareforce.iotvm.eventengine.cep.util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Returns a valid timestamp. The algorithmic searches for a valid timestamps progressively to
 * places that are more likely to provide an accurate timestamp.
 *
 * @author Dimitris Gkoulis
 * @see org.apache.kafka.streams.processor.TimestampExtractor
 * @see org.apache.kafka.streams.processor.FailOnInvalidTimestamp
 * @see org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp
 * @see org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp
 * @see org.apache.kafka.streams.processor.WallclockTimestampExtractor
 * @see org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor
 * @see org.softwareforce.iotvm.eventengine.cep.ct.specifics.FlexibleTimestampExtractor
 */
public final class TimestampExtractorUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimestampExtractorUtil.class);
  private static final List<String> TIMESTAMP_NAMES = new ArrayList<>();

  static {
    // Prioritized!
    TIMESTAMP_NAMES.add("sensed");
    TIMESTAMP_NAMES.add("received");
    TIMESTAMP_NAMES.add("pushed");
    TIMESTAMP_NAMES.add("ingested");
  }

  /* ------------ Constructors ------------ */

  private TimestampExtractorUtil() {}

  /* ------------ Private ------------ */

  private static @Nullable Long checkTimestampsIBO(final @Nullable TimestampsIBO timestampsIBO) {
    if (timestampsIBO == null) {
      return null;
    }
    if (timestampsIBO.getDefaultTimestamp() != null) {
      if (timestampsIBO.getDefaultTimestamp() > 0) {
        return timestampsIBO.getDefaultTimestamp();
      }
    }
    if (timestampsIBO.getTimestamps() == null) {
      return null;
    }
    if (timestampsIBO.getTimestamps().isEmpty()) {
      return null;
    }
    for (final String timestampName : TIMESTAMP_NAMES) {
      if (!timestampsIBO.getTimestamps().containsKey(timestampName)) {
        continue;
      }
      final @Nullable Long candidate = timestampsIBO.getTimestamps().get(timestampName);
      if (candidate == null) {
        continue;
      }
      if (candidate <= 0) {
        continue;
      }
      return candidate;
    }
    return null;
  }

  private static @Nullable Long checkTimestamp(final @Nullable Long timestamp) {
    if (timestamp == null) {
      return null;
    }
    if (timestamp <= 0) {
      return null;
    }
    return timestamp;
  }

  /* ------------ Utilities ------------ */

  /**
   * Extracts a valid timestamp from {@link TimestampsIBO} or from a {@link List} of timestamps. If
   * none of them can provide a valid timestamp, current timestamp ({@code
   * Instant.now().toEpochMilli()}) is returned.
   *
   * <p>Example Usage:
   *
   * <pre>{@code
   * TimestampExtractorUtil.get(ibo.getTimestamps(), List.of(partitionTime));
   * }</pre>
   *
   * @param timestampsIBO the {@link TimestampsIBO} to extract timestamps from.
   * @param moreTimestamps a {@link List} of timestamps to check. Timestamps are checked in the
   *     order of the list.
   * @return a valid timestamp (epoch milli).
   */
  public static long get(TimestampsIBO timestampsIBO, List<Long> moreTimestamps) {
    Long candidate = checkTimestampsIBO(timestampsIBO);

    if (candidate != null) {
      return candidate;
    }

    for (final Long extraTimestamp : moreTimestamps) {
      candidate = checkTimestamp(extraTimestamp);

      if (candidate != null) {
        return candidate;
      }
    }

    return Instant.now().toEpochMilli();
  }
}
