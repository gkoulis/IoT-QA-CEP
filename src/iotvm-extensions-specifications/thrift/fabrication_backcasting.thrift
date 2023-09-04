namespace java org.softwareforce.iotvm.shared.extensions.fabrication_backcasting.spec
namespace py iotvm_extensions.fabrication_backcasting.spec

include "base.thrift"

exception BackcastException {
  1: optional string reason;
}

struct BackcastScope {
  1: string physicalQuantity;
  2: string sensorId;
  3: string topicName;
  4: i64 frequencyInSeconds;
}

struct BackcastRequest {
  1: i64 startTimestamp;
  2: i64 endTimestamp;
  3: optional string comment;
}

struct BackcastResponse {
  1: double value;

  /**
   * UTC Timestamp in milliseconds representing the start of the time window in which the backcasted value belongs.
   */
  2: i64 startTimestamp;

  /**
   * UTC Timestamp in milliseconds representing the end of the time window in which the backcasted value belongs.
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
 * Defines the (fabrication) backcasting service contract.
 */
service FabricationBackcastingService extends base.BaseService {

   /**
    * NOTICE: When active, idempotence must be ensured.
    */
   oneway void ensure(1: BackcastScope scope);

   BackcastResponse backcast(1:BackcastScope scope, 2:BackcastRequest request) throws (1:BackcastException exc);
}
