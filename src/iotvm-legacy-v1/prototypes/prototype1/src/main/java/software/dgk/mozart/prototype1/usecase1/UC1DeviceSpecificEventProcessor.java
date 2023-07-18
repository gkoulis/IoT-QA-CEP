package software.dgk.mozart.prototype1.usecase1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

/**
 * Component for performing complex event processing (CEP) with Kafka streams library.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt Tuesday 15 February 2022
 * @since 1.0.0-PROTOTYPE.1
 */
@SuppressWarnings({"Convert2Lambda", "Convert2Diamond"})
@Component
public class UC1DeviceSpecificEventProcessor {

  private static final String TEMPERATURE_LITERAL = "TEMPERATURE";
  private static final String MOISTURE_LITERAL = "MOISTURE";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
  private static final Serde<Long> LONG_SERDE = Serdes.Long();
  private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();

  private static final JsonDeserializer<UC1DeviceSpecificEvent>
      UC_1_DEVICE_SPECIFIC_EVENT_JSON_DESERIALIZER = new JsonDeserializer<UC1DeviceSpecificEvent>();
  private static final JsonSerializer<UC1DeviceSpecificEvent>
      UC_1_DEVICE_SPECIFIC_EVENT_JSON_SERIALIZER = new JsonSerializer<UC1DeviceSpecificEvent>();
  private static final Serde<UC1DeviceSpecificEvent> UC_1_DEVICE_SPECIFIC_EVENT_SERDE =
      Serdes.serdeFrom(
          UC_1_DEVICE_SPECIFIC_EVENT_JSON_SERIALIZER, UC_1_DEVICE_SPECIFIC_EVENT_JSON_DESERIALIZER);

  private static final JsonDeserializer<UC1DeviceGenericEvent>
      UC_1_DEVICE_GENERIC_EVENT_JSON_DESERIALIZER = new JsonDeserializer<UC1DeviceGenericEvent>();
  private static final JsonSerializer<UC1DeviceGenericEvent>
      UC_1_DEVICE_GENERIC_EVENT_JSON_SERIALIZER = new JsonSerializer<UC1DeviceGenericEvent>();
  private static final Serde<UC1DeviceGenericEvent> UC_1_DEVICE_GENERIC_EVENT_SERDE =
      Serdes.serdeFrom(
          UC_1_DEVICE_GENERIC_EVENT_JSON_SERIALIZER, UC_1_DEVICE_GENERIC_EVENT_JSON_DESERIALIZER);

  private static final JsonDeserializer<UC1GenericAggregateBox>
      UC_1_GENERIC_AGGREGATE_BOX_JSON_DESERIALIZER = new JsonDeserializer<UC1GenericAggregateBox>();
  private static final JsonSerializer<UC1GenericAggregateBox>
      UC_1_GENERIC_AGGREGATE_BOX_JSON_SERIALIZER = new JsonSerializer<UC1GenericAggregateBox>();
  private static final Serde<UC1GenericAggregateBox> UC_1_GENERIC_AGGREGATE_BOX_SERDE =
      Serdes.serdeFrom(
          UC_1_GENERIC_AGGREGATE_BOX_JSON_SERIALIZER, UC_1_GENERIC_AGGREGATE_BOX_JSON_DESERIALIZER);

  static {
    UC_1_DEVICE_SPECIFIC_EVENT_JSON_DESERIALIZER.addTrustedPackages("*");
    UC_1_DEVICE_GENERIC_EVENT_JSON_DESERIALIZER.addTrustedPackages("*");
    UC_1_GENERIC_AGGREGATE_BOX_JSON_DESERIALIZER.addTrustedPackages("*");
  }

  private final UC1DeviceGenericEventFactory uc1DeviceGenericEventFactory;

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String kafkaBootstrapServers;

  public UC1DeviceSpecificEventProcessor(
      UC1DeviceGenericEventFactory uc1DeviceGenericEventFactory) {
    this.uc1DeviceGenericEventFactory = uc1DeviceGenericEventFactory;
  }

  private static String convertUC1GenericAggregateBoxToJsonString(
      UC1GenericAggregateBox aggregateBox) {
    try {
      return OBJECT_MAPPER.writeValueAsString(aggregateBox);
    } catch (JsonProcessingException ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private static UC1GenericAggregateBox convertJsonStringToUC1GenericAggregateBox(String json) {
    try {
      return OBJECT_MAPPER.readValue(json, UC1GenericAggregateBox.class);
    } catch (Exception ex) {
      return null;
    }
  }

  private void createCEPPipelineForQuantity(
      KStream<String, UC1DeviceGenericEvent> kStream, final String quantityType) {
    KStream<Windowed<String>, String> newKStream =
        kStream
            .map(
                new KeyValueMapper<
                    String, UC1DeviceGenericEvent, KeyValue<String, UC1DeviceGenericEvent>>() {

                  @Override
                  public KeyValue<String, UC1DeviceGenericEvent> apply(
                      String ignored, UC1DeviceGenericEvent uc1GenericEvent) {
                    return new KeyValue<>(uc1GenericEvent.getDeviceId(), uc1GenericEvent);
                  }
                })
            // .groupByKey(Grouped.with(STRING_SERDE, UC_1_DEVICE_GENERIC_EVENT_SERDE))
            // Not good but not bad either.
            .groupBy(
                (key, value) -> quantityType,
                Grouped.with(STRING_SERDE, UC_1_DEVICE_GENERIC_EVENT_SERDE))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(1)))
            .aggregate(
                new Initializer<String>() {
                  @Override
                  public String apply() {
                    return convertUC1GenericAggregateBoxToJsonString(new UC1GenericAggregateBox());
                  }
                },
                new Aggregator<String, UC1DeviceGenericEvent, String>() {
                  @Override
                  public String apply(String s, UC1DeviceGenericEvent event, String json) {
                    UC1GenericAggregateBox aggregateBox =
                        convertJsonStringToUC1GenericAggregateBox(json);
                    aggregateBox.data.put(
                        String.format("device_value_%s", event.getDeviceId()), event.getValue());
                    aggregateBox.timestamp = event.getTimestamp();
                    aggregateBox.calculateMean();
                    return convertUC1GenericAggregateBoxToJsonString(aggregateBox);
                  }
                },
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as(
                        String.format(
                            "time-windowed-aggregated-%s-window-store",
                            quantityType.toLowerCase(Locale.ENGLISH)))
                    .withCachingDisabled()
                    .withKeySerde(STRING_SERDE)
                    .withValueSerde(STRING_SERDE))
            .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), BufferConfig.unbounded()))
            .toStream();

    // TODO Debug.
    // newKStream.print(Printed.toSysOut());

    KStream<String, String> newKStream2 =
        newKStream
            .map(
                new KeyValueMapper<Windowed<String>, String, KeyValue<String, String>>() {

                  @Override
                  public KeyValue<String, String> apply(Windowed<String> k, String v) {
                    if (k == null || v == null) {
                      return new KeyValue<>(
                          UC1Constants.JSON_NONE_VALUE, UC1Constants.JSON_NONE_VALUE);
                    }

                    UC1GenericAggregateBox aggregateBox =
                        convertJsonStringToUC1GenericAggregateBox(v);
                    if (aggregateBox == null) {
                      return new KeyValue<>(
                          UC1Constants.JSON_NONE_VALUE, UC1Constants.JSON_NONE_VALUE);
                    }

                    if (k.window() == null) {
                      return new KeyValue<>(
                          UC1Constants.JSON_NONE_VALUE, UC1Constants.JSON_NONE_VALUE);
                    }

                    final ObjectNode key = OBJECT_MAPPER.createObjectNode();
                    key.put("quantityType", quantityType);
                    key.put("startTimestamp", k.window().start());
                    key.put("startTimestampString", k.window().startTime().toString());
                    key.put("endTimestamp", k.window().end());
                    key.put("endTimestampString", k.window().endTime().toString());
                    key.put("clientTimestamp", aggregateBox.timestamp);
                    key.put("clientTimestampString", aggregateBox.timestampToInstant().toString());

                    final ObjectNode value = OBJECT_MAPPER.createObjectNode();
                    value.put("value", aggregateBox.mean);

                    String keyJSON;
                    try {
                      keyJSON = OBJECT_MAPPER.writeValueAsString(key);
                    } catch (JsonProcessingException e) {
                      keyJSON = UC1Constants.JSON_NONE_VALUE;
                    }

                    String valueJSON;
                    try {
                      valueJSON = OBJECT_MAPPER.writeValueAsString(value);
                    } catch (JsonProcessingException e) {
                      valueJSON = UC1Constants.JSON_NONE_VALUE;
                    }

                    return new KeyValue<>(keyJSON, valueJSON);
                  }
                })
            .filter(
                (k, v) ->
                    !k.equals(UC1Constants.JSON_NONE_VALUE)
                        && !v.equals(UC1Constants.JSON_NONE_VALUE));

    // TODO Debug.
    // newKStream2.print(Printed.toSysOut());

    newKStream2.to(
        UC1Constants.prefixedVersionedTopic(
            quantityType.toLowerCase(Locale.ENGLISH) + "-update-event", 1));
  }

  @Autowired
  public void build(StreamsBuilder streamsBuilder) {
    final KStream<String, UC1DeviceSpecificEvent> stringUC1DeviceSpecificEventKStream =
        streamsBuilder.stream(
            UC1Constants.SENSOR_TELEMETRY_EVENT_TOPIC,
            Consumed.with(STRING_SERDE, UC_1_DEVICE_SPECIFIC_EVENT_SERDE));

    KStream<String, UC1DeviceGenericEvent> deviceGenericEventKStream =
        stringUC1DeviceSpecificEventKStream
            .flatMapValues(
                value -> {
                  UC1DeviceGenericEvent temperature =
                      uc1DeviceGenericEventFactory.createTemperature(value);
                  UC1DeviceGenericEvent moisture =
                      uc1DeviceGenericEventFactory.createMoisture(value);
                  return Arrays.asList(temperature, moisture);
                })
            .map(
                new KeyValueMapper<
                    String, UC1DeviceGenericEvent, KeyValue<String, UC1DeviceGenericEvent>>() {

                  @Override
                  public KeyValue<String, UC1DeviceGenericEvent> apply(
                      String ignored, UC1DeviceGenericEvent uc1GenericEvent) {
                    return new KeyValue<>(uc1GenericEvent.getDeviceId(), uc1GenericEvent);
                  }
                });

    KStream<String, UC1DeviceGenericEvent> temperatureEventKStream =
        deviceGenericEventKStream.filter(
            (key, value) -> TEMPERATURE_LITERAL.equals(value.getKey()));

    createCEPPipelineForQuantity(temperatureEventKStream, TEMPERATURE_LITERAL);

    KStream<String, UC1DeviceGenericEvent> moistureEventKStream =
        deviceGenericEventKStream.filter((key, value) -> MOISTURE_LITERAL.equals(value.getKey()));

    createCEPPipelineForQuantity(moistureEventKStream, MOISTURE_LITERAL);
  }
}
