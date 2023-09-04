package org.softwareforce.iotvm.eventengine.extensions;

import java.util.Optional;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.configuration.ApplicationConfiguration;
import org.softwareforce.iotvm.shared.extensions.base.BaseService;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.FabricationForecastingService;
import org.softwareforce.iotvm.shared.extensions.sensing_recording.spec.SensingRecordingService;

/**
 * Singleton factory for managing extensions clients.
 *
 * <p>It is not thread safe. It is highly recommended to initialize the required services and use
 * them as dependencies in application beans.
 *
 * @author Dimitris Gkoulis
 */
public final class ExtensionsClientsFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExtensionsClientsFactory.class);

  private static final ExtensionsClientsFactory INSTANCE = new ExtensionsClientsFactory();

  private String host;
  private int port;
  private TTransport tTransport;
  private TProtocol tProtocol;

  private FabricationForecastingService.Client fabricationForecastingServiceClient;
  private SensingRecordingService.Client sensingRecordingServiceClient;

  /* ------------ Constructors ------------ */

  private ExtensionsClientsFactory() {
    this.host = null;
    this.port = 0;
    this.tTransport = null;
    this.tProtocol = null;
    this.fabricationForecastingServiceClient = null;
    this.sensingRecordingServiceClient = null;
  }

  public static synchronized ExtensionsClientsFactory getInstance() {
    return INSTANCE;
  }

  /* ------------ Internal ------------ */

  private synchronized void close() {
    if (this.tTransport != null) {
      this.tTransport.close();
    }
    this.tTransport = null;
    this.tProtocol = null;
    this.fabricationForecastingServiceClient = null;
    this.sensingRecordingServiceClient = null;
  }

  private synchronized void reconnect() throws TException {
    this.host = ApplicationConfiguration.getInstance().getExtensionsHost();
    this.port = ApplicationConfiguration.getInstance().getExtensionsPort();

    this.tTransport = new TSocket(this.host, this.port);
    this.tTransport.open();

    /*
    final String url = "http://" + host + ":" + port;
    this.tTransport = new THttpClient(url);
    this.tTransport.open();
    */

    this.tProtocol = new TBinaryProtocol(this.tTransport);

    final TMultiplexedProtocol tMultiplexedProtocol1 =
        new TMultiplexedProtocol(this.tProtocol, "FabricationForecastingService");
    this.fabricationForecastingServiceClient =
        new FabricationForecastingService.Client(tMultiplexedProtocol1);
    // this.fabricationForecastingServiceClient =
    // ReconnectingThriftClient.wrap(this.fabricationForecastingServiceClient, new
    // ReconnectingThriftClient.Options(1, 100));

    final TMultiplexedProtocol tMultiplexedProtocol2 =
        new TMultiplexedProtocol(this.tProtocol, "SensingRecordingService");
    this.sensingRecordingServiceClient = new SensingRecordingService.Client(tMultiplexedProtocol2);
    // this.sensingRecordingServiceClient =
    // ReconnectingThriftClient.wrap(this.sensingRecordingServiceClient);
  }

  private void tryToReconnect() {
    this.close();
    try {
      this.reconnect();
    } catch (TException ex) {
      this.close();
      LOGGER.error(
          "Failed to reconnect to extensions server {}:{}. Reason : {}. Services will be"
              + " unavailable.",
          this.host,
          this.port,
          ex.getMessage());
    }
  }

  private boolean ping(BaseService.Client client) {
    if (client == null) {
      return false;
    }
    try {
      client.ping();
      return true;
    } catch (TTransportException ex) {
      LOGGER.warn("TTransportException: {} (type : {})", ex.getMessage(), ex.getType());
      return false;
    } catch (TException ex) {
      LOGGER.warn("TException: {}", ex.getMessage());
      return false;
    }
  }

  /* ------------ Getters ------------ */

  public Optional<FabricationForecastingService.Client> getFabricationForecastingServiceClient() {
    final boolean pong = this.ping(this.fabricationForecastingServiceClient);
    if (!pong) {
      this.tryToReconnect();
    }
    return Optional.ofNullable(this.fabricationForecastingServiceClient);
  }

  public Optional<SensingRecordingService.Client> getSensingRecordingServiceClient() {
    final boolean pong = this.ping(this.sensingRecordingServiceClient);
    if (!pong) {
      this.tryToReconnect();
    }
    return Optional.ofNullable(this.sensingRecordingServiceClient);
  }
}
