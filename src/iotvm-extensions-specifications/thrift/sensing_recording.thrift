namespace java org.softwareforce.iotvm.shared.extensions.sensing_recording.spec
namespace py iotvm_extensions.sensing_recording.spec

include "base.thrift"

struct RecordedSensorMeasurement {
  1: string name;
  2: double value;
  3: string unit;
}

struct RecordedSensorData {
  1: string sensorId;
  2: list<RecordedSensorMeasurement> measurements;
  3: i64 timestamp;
  4: map<string, string> additional;
}

struct PhysicalQuantityDataPoint {
  1: double value;
  2: i64 timestamp;
}

/**
 * Defines the sensing recording service contract.
 */
service SensingRecordingService extends base.BaseService {

   oneway void recordSensorData(1: RecordedSensorData recordedSensorData);

   # list<PhysicalQuantityDataPoint> getPhysicalQuantityTimeSeries(1: string sensorId, 2: string physicalQuantity, 3: i64 fromTimestamp, 4: i64 toTimestamp, 5: bool ascending);

   list<PhysicalQuantityDataPoint> getBasicAggregationsCTFReals(1: list<string> sensorIds, 2: string physicalQuantity, 3: i64 fromTimestamp, 4: i64 toTimestamp);
   // TODO rename: getCloseValuesInInterval

   // TODO Get real average!?????? νομίζω είναι καλύτερα. Γιατί έτσι μπορώ να το κάνω abstract στον απόλυτο βαθμό.... επίσης, λέω να το πάιρνω από ΟΛΕΣ τις διαθέσιμες πηγές δεδομένων...
}
