package org.softwareforce.iotvm.gateway.monitor;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseBroadcaster;
import jakarta.ws.rs.sse.SseEventSink;

/**
 * Resource for exposing monitoring endpoints.
 *
 * @author Dimitris Gkoulis
 */
@Path("/api/v1/monitoring")
public class MonitorResource {

  private Sse sse = null;
  private SseBroadcaster sseBroadcaster = null;
  private OutboundSseEvent.Builder outboundSseEventBuilder = null;
  private MonitorConsumer monitorConsumer = null;

  /* ---------------- Constructors -------------- */

  public MonitorResource() {}

  /* ---------------- Jakarta -------------- */

  @Context
  public void setSse(Sse sse) {
    this.sse = sse;
    this.sseBroadcaster = sse.newBroadcaster();
    this.outboundSseEventBuilder = sse.newEventBuilder();

    this.monitorConsumer = new MonitorConsumer(this.outboundSseEventBuilder, this.sseBroadcaster);
    this.monitorConsumer.initAndStart();
  }

  /* ---------------- Endpoints -------------- */

  @GET
  @Path("/subscribe")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  public void subscribeToMonitoringEvents(@Context SseEventSink sseEventSink, @Context Sse sse) {
    // TODO Fix better.
    if (this.sse == null) {
      this.setSse(sse);
    }
    // sseEventSink.send(sse.newEvent("Welcome !"));
    this.sseBroadcaster.register(sseEventSink);
    // sseEventSink.send(sse.newEvent("You are registred !"));
  }
}
