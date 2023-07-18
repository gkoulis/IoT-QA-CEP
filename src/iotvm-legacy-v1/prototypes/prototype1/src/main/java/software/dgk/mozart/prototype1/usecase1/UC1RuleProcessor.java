package software.dgk.mozart.prototype1.usecase1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Component for processing business rules (rule engine).
 *
 * @author Dimitris Gkoulis
 * @createdAt Tuesday 15 February 2022
 * @lastModifiedAt Wednesday 16 February 2022
 * @since 1.0.0-PROTOTYPE.1
 */
@SuppressWarnings({"Convert2Lambda"})
@Component
public class UC1RuleProcessor {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Serde<String> STRING_SERDE = Serdes.String();

  private static final ScriptEngine SCRIPT_ENGINE =
      new ScriptEngineManager().getEngineByName("nashorn");

  private static final String TOPIC =
      UC1Constants.prefixedVersionedTopic(UC1Constants.TEMPERATURE_UPDATE_EVENT_TOPIC, 1);

  private String mainScript = "";

  public UC1RuleProcessor() {
    try {
      this.readMainScript();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readMainScript() throws IOException {
    // TODO Get from application properties.
    Path pathToFile =
        Path.of(
            "F:\\projects\\DGK-monorepo\\source\\mozart\\prototypes\\prototype1\\src\\main\\"
                + "resources\\main_script.js");
    final String newMainScript = Files.readString(pathToFile);
    if (!this.mainScript.equals(newMainScript)) {
      this.mainScript = newMainScript;
      System.out.println(this.mainScript);
    }
  }

  // TODO Move... to utils -> . <-
  private static ObjectNode convertStringToObjectNode(String json) {
    JsonNode valueJsonNode;
    ObjectNode valueObjectNode;
    try {
      valueJsonNode = OBJECT_MAPPER.readTree(json);
      valueObjectNode = (ObjectNode) valueJsonNode;
      return valueObjectNode;
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

  private boolean binaryDecision(String script, Map<String, Object> data)
      throws ScriptException, NoSuchMethodException {
    SCRIPT_ENGINE.eval(script);
    final Invocable invocable = (Invocable) SCRIPT_ENGINE;
    Object funcResult = invocable.invokeFunction("performBinaryDecision", data);
    if (funcResult == null) {
      return false;
    }
    if (funcResult instanceof Boolean) {
      return (Boolean) funcResult;
    }
    return false;
  }

  private boolean binaryDecisionWrapper(String script, Map<String, Object> data) {
    try {
      return this.binaryDecision(script, data);
    } catch (ScriptException | NoSuchMethodException e) {
      e.printStackTrace();
      return false;
    }
  }

  // @KafkaListener(topics = "uc1-temperature-update-event-1")
  public void temperatureUpdateEvent1(
      @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Payload String payload) {
    ObjectNode keyObjectNode = convertStringToObjectNode(key);
    ObjectNode valueObjectNode = convertStringToObjectNode(payload);
    System.out.println("DOOR: " + valueObjectNode.get("value").asDouble());
  }

  @Autowired
  public void build(StreamsBuilder streamsBuilder) {
    final KStream<String, String> stream =
        streamsBuilder.stream(
            "uc1-temperature-update-event-1", Consumed.with(STRING_SERDE, STRING_SERDE));

    final KStream<String, String> stream2 = stream
        .filter(
            (key, value) -> {
              final Map<String, Object> data = new HashMap<>();
              data.put("key", key);
              data.put("value", value);
              return this.binaryDecisionWrapper(mainScript, data);
            })
        .map(
            new KeyValueMapper<String, String, KeyValue<String, String>>() {

              @Override
              public KeyValue<String, String> apply(String k, String v) {
                // final ObjectNode key = OBJECT_MAPPER.createObjectNode();

                final ObjectNode value = OBJECT_MAPPER.createObjectNode();
                value.put("intent", true);

                /*
                String keyJSON;
                try {
                  keyJSON = OBJECT_MAPPER.writeValueAsString(key);
                } catch (JsonProcessingException e) {
                  keyJSON = UC1Constants.JSON_NONE_VALUE;
                }
                */

                String valueJSON;
                try {
                  valueJSON = OBJECT_MAPPER.writeValueAsString(value);
                } catch (JsonProcessingException e) {
                  valueJSON = UC1Constants.JSON_NONE_VALUE;
                }

                return new KeyValue<>(k, valueJSON);
              }
            })
        .filter(
            (k, v) ->
                !k.equals(UC1Constants.JSON_NONE_VALUE) && !v.equals(UC1Constants.JSON_NONE_VALUE));

    stream2.print(Printed.toSysOut());

    stream2.to(UC1Constants.prefixedVersionedTopic("door-state-update", 1));
  }

  /* ------------ Scheduled Tasks ------------ */

  @Scheduled(fixedDelay = 12000)
  public void reloadMainScript() {
    try {
      this.readMainScript();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
