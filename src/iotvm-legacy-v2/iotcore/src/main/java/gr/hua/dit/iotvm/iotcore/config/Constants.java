package gr.hua.dit.iotvm.iotcore.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Constants.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt Saturday 14 January 2023
 * @since 1.0.0-PROTOTYPE.1
 */
public final class Constants {

    public static final String UNDEFINED = "undefined";
    public static final String ANY_NODE = "any_node";

    public static final String SENSOR_TELEMETRY_EVENT_TOPIC = "sensor_telemetry_event_0001";
    public static final String SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC = "sensor_telemetry_measurement_event_0001";
    public static final String WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_TOPIC =
            "windowed_average_measurement_value_event_0001";

    /**
     * The names of the enabled measurements. Each name corresponds to a different topic.
     */
    public static final List<MeasurementType> ENABLED_MEASUREMENT_TYPES = new ArrayList<>();

    static {
        ENABLED_MEASUREMENT_TYPES.add(MeasurementType.TEMPERATURE);
        ENABLED_MEASUREMENT_TYPES.add(MeasurementType.HUMIDITY);
    }

    /* ------------ Constructors ------------ */

    private Constants() {}

    /* ------------ Enumerations ------------ */

    public enum MeasurementType {
        TEMPERATURE("temperature", "CELSIUS"),
        HUMIDITY("humidity", "PERCENTAGE");

        private final String name;
        private final String unit;

        MeasurementType(String name, String unit) {
            this.name = name;
            this.unit = unit;
        }

        public String getName() {
            return this.name;
        }

        public String getUnit() {
            return unit;
        }

        /** <strong>Important:</strong> It must follow the convention defined in {@link #SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC}. */
        public String getSensorTelemetryMeasurementEventTopic() {
            return "sensor_telemetry_" + this.name + "_measurement_event_0001";
        }

        /**
         * <strong>Important:</strong> It must follow the convention defined in {@link #WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_TOPIC}.
         *
         * @see #getWindowedAverageMeasurementValueEventTopicWithSuffix(String)
         */
        public String getWindowedAverageMeasurementValueEventTopic() {
            // windowed_average_measurement_value_event_0001
            return "windowed_average_" + this.name + "_measurement_value_event_0001";
        }

        /** <strong>Important:</strong> It must follow the convention defined in {@link #WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_TOPIC}.
         *
         * @see #getWindowedAverageMeasurementValueEventTopic()
         */
        public String getWindowedAverageMeasurementValueEventTopicWithSuffix(String suffix) {
            // windowed_average_measurement_value_event_0001
            return "windowed_average_" + this.name + "_measurement_value_event_0001_" + suffix;
        }
    }
}
