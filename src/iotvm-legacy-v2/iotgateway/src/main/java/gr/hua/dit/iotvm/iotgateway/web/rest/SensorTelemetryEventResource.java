package gr.hua.dit.iotvm.iotgateway.web.rest;

import gr.hua.dit.iotvm.iotgateway.service.SensorTelemetryEventService;
import gr.hua.dit.iotvm.iotgateway.service.model.PushSensorTelemetryEventRequest;
import gr.hua.dit.iotvm.iotgateway.service.model.PushSensorTelemetryEventResult;
import gr.hua.dit.iotvm.iotgateway.web.rest.util.WebRestHeaderUtils;
import jakarta.validation.Valid;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST Controller for managing {@link gr.hua.dit.iotvm.iotgateway.model.SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@RestController
@RequestMapping("/api/v1/http-transport")
public class SensorTelemetryEventResource {

    private final Logger log = LoggerFactory.getLogger(SensorTelemetryEventResource.class);

    private static final String ENTITY_NAME = "sensorTelemetryEvent";

    @Value("${spring.application.name}")
    private String applicationName;

    private final SensorTelemetryEventService sensorTelemetryEventService;

    /* ------------ Constructors ------------ */

    public SensorTelemetryEventResource(SensorTelemetryEventService sensorTelemetryEventService) {
        this.sensorTelemetryEventService = sensorTelemetryEventService;
    }

    /* ---------------- Endpoints -------------- */

    /**
     * {@code POST /api/v1/http-transport/sensor-telemetry-events} : Push a {@link gr.hua.dit.iotvm.iotgateway.model.SensorTelemetryEvent} to Event Bus (i.e., the Kafka).
     *
     * @param pushSensorTelemetryEventRequest the request DTO.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new
     *     entity.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     * @future TODO Valid not working.
     */
    @PostMapping("/sensor-telemetry-events")
    public ResponseEntity<PushSensorTelemetryEventResult> pushSensorTelemetryEvent(
            @RequestBody @Valid PushSensorTelemetryEventRequest pushSensorTelemetryEventRequest)
            throws URISyntaxException {
        log.debug("REST Request to create SensorTelemetryEvent : {}", pushSensorTelemetryEventRequest);
        PushSensorTelemetryEventResult result =
                this.sensorTelemetryEventService.pushSensorTelemetryEvent(pushSensorTelemetryEventRequest);
        // TODO Replace "undefined". Also, add support for UUID in both IoT Core and IoT Gateway.
        return ResponseEntity.created(new URI("/api/v1/sensor-telemetry-event/" + "undefined"))
                .headers(WebRestHeaderUtils.createEntityCreationAlert(
                        this.applicationName, true, ENTITY_NAME, "undefined"))
                .body(result);
    }
}
