namespace java org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec
namespace py iotvm_extensions.fabrication_forecasting.spec

include "base.thrift"

exception ForecastException {
  1: optional string reason;
}

struct ForecastScope {
  1: string physicalQuantity;
  2: string sensorId;
  3: string topicName;
  4: i64 frequencyInSeconds;
}

struct ForecastRequest {
  1: i64 startTimestamp;
  2: i64 endTimestamp;
  3: i64 stepsAhead;
  4: optional string comment;
}

struct ForecastResponse {
  1: double value;

  /**
   * UTC Timestamp in milliseconds representing the start of the time window in which the forecasted value belongs.
   */
  2: i64 startTimestamp;

  /**
   * UTC Timestamp in milliseconds representing the end of the time window in which the forecasted value belongs.
   */
  3: i64 endTimestamp;

  /**
   * Any metric regarding the model.
   *
   * Some metrics refer to score functions or loss functions.
   * Score functions: the bigger value the better.
   * Loss functions: the smaller value the better.
   * Negative numbers may indicate bad data fitting.
   * Positive or negative inf may also indicate bad data fitting.
   */
  4: map<string, double> metrics;
}

/**
 * Defines the (fabrication) forecasting service contract.
 */
service FabricationForecastingService extends base.BaseService {

   /**
    * NOTICE: When active, idempotence must be ensured.
    */
   oneway void ensure(1: ForecastScope scope);

   ForecastResponse forecast(1:ForecastScope scope, 2:ForecastRequest request) throws (1:ForecastException exc);
}
