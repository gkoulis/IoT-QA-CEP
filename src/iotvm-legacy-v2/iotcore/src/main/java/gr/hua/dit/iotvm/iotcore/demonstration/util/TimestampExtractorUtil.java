package gr.hua.dit.iotvm.iotcore.demonstration.util;

import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for the implementations of {@link org.apache.kafka.streams.processor.TimestampExtractor}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 14 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class TimestampExtractorUtil {

    private static final Logger logger = LoggerFactory.getLogger(TimestampExtractorUtil.class);

    /* ------------ Constructors ------------ */

    private TimestampExtractorUtil() {}

    /* ------------ Utilities ------------ */

    public static long getTimestamp(Long timestamp, long previousTimestamp) {
        if (timestamp == null) {
            long timestampNow = Instant.now().toEpochMilli();
            logger.debug("timestamp=null, previousTimestamp={}, timestampNow={}", previousTimestamp, timestampNow);
            return timestampNow;

            // IMPORTANT: I will ignore previousTimestamp for now.
            // It does not seem a valid timestamp since it is always a past datetime!
            // @future Ensure that this is right and remove this statement.
            /*
            if (previousTimestamp == -1L) {
                return timestampNow;
            } else {
                return previousTimestamp;
            }
            */
        } else {
            // @future Should I make more validations?
            // @future I think that the best is that clients provide a string timestamp.
            //     Then, the CEP will try to parse it.
            //     If it's valid it will use it, otherwise it will set the current timestamp.
            return timestamp;
        }
    }
}
