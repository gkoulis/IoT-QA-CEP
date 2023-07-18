package org.softwareforce.iotvm.gateway.controller;

import jakarta.validation.Valid;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.gateway.controller.model.PushSensorTelemetryEventWebRequest;
import org.softwareforce.iotvm.gateway.controller.model.PushSensorTelemetryEventWebResponse;
import org.softwareforce.iotvm.gateway.service.SensorTelemetryEventService;
import org.softwareforce.iotvm.gateway.service.model.PushSensorTelemetryEventRequest;
import org.softwareforce.iotvm.gateway.service.model.PushSensorTelemetryEventResult;

/**
 * REST controller for managing sensor telemetry events.
 *
 * @author Dimitris Gkoulis
 */
@Path("/api/v1/http-transport")
@Produces(MediaType.APPLICATION_JSON)
public class SensorTelemetryEventController {

  private final Logger log = LoggerFactory.getLogger(SensorTelemetryEventController.class);

  private final SensorTelemetryEventService sensorTelemetryEventService;

  /* ------------ Constructors ------------ */

  public SensorTelemetryEventController(SensorTelemetryEventService sensorTelemetryEventService) {
    this.sensorTelemetryEventService = sensorTelemetryEventService;
  }

  /* ---------------- Endpoints -------------- */

  /**
   * {@code POST /api/v1/http-transport/sensor-telemetry-events} : Push a {@code
   * SensorTelemetryEvent} to Event Bus (i.e., the Kafka).
   *
   * @param pushSensorTelemetryEventWebRequest the request DTO.
   * @return the pushed event.
   */
  @POST
  @Path("/sensor-telemetry-events")
  public Response pushSensorTelemetryEvent(
      @Valid PushSensorTelemetryEventWebRequest pushSensorTelemetryEventWebRequest) {
    log.debug("HTTP Request to push SensorTelemetryEvent : {}", pushSensorTelemetryEventWebRequest);
    final PushSensorTelemetryEventRequest request =
        new PushSensorTelemetryEventRequest(
            pushSensorTelemetryEventWebRequest.getSensorTelemetryEvent(),
            Instant.now().toEpochMilli());
    final PushSensorTelemetryEventResult result =
        this.sensorTelemetryEventService.pushSensorTelemetryEvent(request);
    final PushSensorTelemetryEventWebResponse response =
        new PushSensorTelemetryEventWebResponse(request.getSensorTelemetryEvent());
    return Response.ok().entity(response).build();
  }
}
