package gr.hua.dit.iotvm.iotgateway.service;

import gr.hua.dit.iotvm.iotgateway.config.Constants;
import gr.hua.dit.iotvm.iotgateway.service.model.PushSensorTelemetryEventRequest;
import gr.hua.dit.iotvm.iotgateway.service.model.PushSensorTelemetryEventResult;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

/**
 * Service for managing {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Service
public class SensorTelemetryEventService {

    private final Logger log = LoggerFactory.getLogger(SensorTelemetryEventService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /* ------------ Constructors ------------ */

    public SensorTelemetryEventService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /* ------------ Logic ------------ */

    // TODO @Valid not working.
    public PushSensorTelemetryEventResult pushSensorTelemetryEvent(
            @Valid PushSensorTelemetryEventRequest pushSensorTelemetryEventRequest) {
        log.info("Request to push SensorTelemetryEvent : {}", pushSensorTelemetryEventRequest);
        Assert.notNull(pushSensorTelemetryEventRequest, "pushSensorTelemetryEventRequest cannot be null!");
        Assert.notNull(
                pushSensorTelemetryEventRequest.getSensorTelemetryEvent(),
                "pushSensorTelemetryEventRequest.sensorTelemetryEvent cannot be null!");
        // TODO More validations and more cleanings.
        SensorTelemetryEvent sensorTelemetryEvent = pushSensorTelemetryEventRequest.getSensorTelemetryEvent();

        // TODO Temporary bug fix: use setters and class methods to correct and validate data.
        //  WHO IS RESPONSIBLE FOR SETTING THIS VALUE? INGESTION SERVICE IN CEP? CEP? GATEWAY?
        //  Προτείνω να βάζω gateway_timestamp στα extra data ή στα metadata
        //  και να αφήνω το CEP να παίρνει τις αποφάσεις.
        /*
        if (sensorTelemetryEvent.getTimestamp() == null) {
            sensorTelemetryEvent = new SensorTelemetryEvent(
                    sensorTelemetryEvent.getMeasurements(),
                    sensorTelemetryEvent.getNodeId(),
                    sensorTelemetryEvent.getNodeGroupId(),
                    Instant.now().toEpochMilli(),
                    sensorTelemetryEvent.getExtraData());
        }
        */
        this.kafkaTemplate.send(Constants.SENSOR_TELEMETRY_EVENT_TOPIC, sensorTelemetryEvent);
        final PushSensorTelemetryEventResult pushSensorTelemetryEventResult = new PushSensorTelemetryEventResult();
        pushSensorTelemetryEventResult.setSensorTelemetryEvent(sensorTelemetryEvent);
        return pushSensorTelemetryEventResult;
    }
}
