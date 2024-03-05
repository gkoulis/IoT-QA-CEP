<script setup>
import moment from "moment";
import { computed, ref, onMounted, onBeforeUnmount } from "vue";

const measurements = ref([]);
const measurementsAvg = ref([]);
const lastMeasurementsAvg = ref(null);
const registeredSensorIds = ref([
  "sensor-1",
  "sensor-2",
  "sensor-3",
  "sensor-4",
  "sensor-5",
  "sensor-6",
]);
const ctpOptions = ref([]);
const ctpOptionRef = ref(null);
const debugRef = ref(false);
const REGISTERED = [
  // "w_avg_temperature_PT5S_null_null_2_true_0_PT5S_0",
  // "w_avg_temperature_PT5S_null_null_2_true_4_PT5S_0",
  // "w_avg_temperature_PT5S_null_null_2_true_0_PT5S_4",
  // "w_avg_temperature_PT5S_null_null_2_true_4_PT5S_4",
  // // 4 Sensors
  // "w_avg_temperature_PT5S_null_null_4_true_0_PT5S_0",
  // "w_avg_temperature_PT5S_null_null_4_true_4_PT5S_0",
  // "w_avg_temperature_PT5S_null_null_4_true_0_PT5S_4",
  // "w_avg_temperature_PT5S_null_null_4_true_4_PT5S_4",
  // // 6 Sensors
  // "w_avg_temperature_PT5S_null_null_6_true_0_PT5S_0",
  // "w_avg_temperature_PT5S_null_null_6_true_4_PT5S_0",
  // "w_avg_temperature_PT5S_null_null_6_true_0_PT5S_4",
  // "w_avg_temperature_PT5S_null_null_6_true_4_PT5S_4",
];

const measurementsAvgToDisplay = computed(() => {
  if (!ctpOptionRef.value) {
    return [];
  }
  return measurementsAvg.value.filter((i) => i.real.compositeTransformationParametersIdentifier === ctpOptionRef.value);
})

let eventSource = null;

onMounted(() => {
  eventSource = new EventSource(
    "http://localhost:9001/application/api/v1/monitoring/subscribe",
    { withCredentials: true }
  );
  eventSource.onopen = function (event) {
    console.log(event);
  };

  eventSource.addEventListener(
    "monitoringEvent",
    function (event) {
      const eventData = JSON.parse(event.data);

      if (eventData.topicName === "ga.sensor_telemetry_measurements_average_event.0001") {
        if (REGISTERED.length > 0 && !REGISTERED.includes(eventData.real.compositeTransformationParametersIdentifier)) {
          return;
        }
        measurementsAvg.value.push(eventData);
        lastMeasurementsAvg.value = eventData;
        if (!ctpOptions.value.includes(eventData.real.compositeTransformationParametersIdentifier)) {
          ctpOptions.value.push(eventData.real.compositeTransformationParametersIdentifier)
        }
      } else if (eventData.topicName === "ga.sensor_telemetry_measurement_event.0001") {
        // TODO Temporarily disabled.
        // measurements.value.push(eventData);
      }
    },
    false
  );

  eventSource.onmessage = function (event) {
    console.log(event);
    append(event.data);
  };

  function append(data) {
    var ul = document.getElementById("data");
    var li = document.createElement("li");
    li.appendChild(document.createTextNode(data));
    ul.insertBefore(li, ul.childNodes[0]);
  }
});

onBeforeUnmount(() => {
  eventSource.close();
});

const formatDateTime = (timestamp) => {
  const dt = new Date(timestamp);
  return moment(dt).format("HH:mm:ss");
};
</script>

<template>
  <main class="bg-gray-200 min-h-full">
    <header class="py-6 bg-black border-t border-t-gray-600">
      <div class="mx-auto px-4">
        <h1 class="text-3xl font-bold tracking-tight text-white">Stream</h1>
      </div>
    </header>

    <!-- mx-auto max-w-7xl -->
    <div class="my-4 mx-auto px-4 sm:px-6 lg:px-8">
      <!-- mx-auto max-w-5xl -->
      <div class="mx-auto">
        <div class="space-y-4">
          <!--
          <div class="bg-white">
            <table class="CommonSimpleTable CommonSimpleTable--SmallFont">
              <thead>
                <tr>
                  <th colspan="2">parameters</th>
                </tr>
                <tr>
                  <th>name</th>
                  <th>value</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td class="font-semibold">physicalQuantity</td>
                  <td>PhysicalQuantity.TEMPERATURE</td>
                </tr>
                <tr>
                  <td class="font-semibold">timeWindowSize</td>
                  <td>Duration.ofSeconds(30)</td>
                </tr>
                <tr>
                  <td class="font-semibold">timeWindowGrace</td>
                  <td>null</td>
                </tr>
                <tr>
                  <td class="font-semibold">timeWindowAdvance</td>
                  <td>null</td>
                </tr>
                <tr>
                  <td class="font-semibold">
                    minimumNumberOfContributingSensors
                  </td>
                  <td>4</td>
                </tr>
                <tr>
                  <td class="font-semibold">ignoreCompletenessFiltering</td>
                  <td>true</td>
                </tr>
                <tr>
                  <td class="font-semibold">pastWindowsLookup</td>
                  <td>1</td>
                </tr>
              </tbody>
            </table>
          </div>
          -->

          <div class="bg-white">
            <div class="flex space-x-2 p-2">
              <select class="border p-0.5 text-xs" v-model="ctpOptionRef">
                <option v-for="ctpOption in ctpOptions" :key="ctpOption" :value="ctpOption" :label="ctpOption">{{ ctpOption }}</option>
              </select>
              <button class="border p-0.5 text-xs" @click="debugRef = !debugRef">toggle debug ({{ debugRef }})</button>
            </div>
          </div>

          <div class="bg-white">
            <table class="CommonSimpleTable">
              <thead>
                <tr>
                  <th colspan="2" v-if="debugRef">debug</th>
                  <th colspan="2">time window</th>
                  <th colspan="3">average</th>
                  <th colspan="4">quality properties</th>
                  <th colspan="2">past events</th>
                  <th colspan="2">forecasted events</th>
                  <th colspan="6" v-if="debugRef">sensors</th>
                </tr>
                <tr>
                  <th v-if="debugRef">cycle</th>
                  <th v-if="debugRef">recurring window</th>
                  <th>start</th>
                  <th>end</th>
                  <th>name</th>
                  <th>value</th>
                  <th>real</th>
                  <th>completeness</th>
                  <th>timeliness1</th>
                  <th>timeliness2 (alt)</th>
                  <th>accuracy</th>
                  <th>count</th>
                  <th>duration</th>
                  <th>count</th>
                  <th>duration</th>

                  <!-- sensors -->
                  <!-- todo dynamically -->
                  <th v-if="debugRef">1</th>
                  <th v-if="debugRef">2</th>
                  <th v-if="debugRef">3</th>
                  <th v-if="debugRef">4</th>
                  <th v-if="debugRef">5</th>
                  <th v-if="debugRef">6</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="measurementAvg in measurementsAvgToDisplay"
                  :key="measurementAvg.real.identifiers.clientSideId.string"
                >
                  <td v-if="debugRef">
                    {{
                      measurementAvg.real.additional
                        ?.debugStringUniqueCycleValues?.string
                    }}
                  </td>
                  <td v-if="debugRef">
                    {{
                      measurementAvg.real.additional
                        ?.debugStringUniqueRecurringWindowValues?.string
                    }}
                  </td>
                  <td>
                    {{ formatDateTime(measurementAvg.real.startTimestamp) }}
                  </td>
                  <td>
                    {{ formatDateTime(measurementAvg.real.endTimestamp) }}
                  </td>
                  <td>{{ measurementAvg.real.average.name }}</td>
                  <td :title="measurementAvg.real.average.value.double">
                    {{ measurementAvg.real.average.value.double.toFixed(2) }}
                  </td>
                  <td :title="measurementAvg.real.additional.realAverage.double">
                    {{ measurementAvg.real.additional.realAverage.double.toFixed(2) }}
                  </td>
                  <td>
                    {{
                      (
                        measurementAvg.real.qualityProperties.metrics
                          ?.completeness1?.double * 100
                      ).toFixed(2)
                    }}
                    %
                  </td>
                  <td>
                    {{
                      (
                        measurementAvg.real.qualityProperties.metrics
                          ?.timeliness1?.double * 100
                      ).toFixed(2)
                    }}
                    %
                  </td>
                  <td>
                    {{
                      (
                        measurementAvg.real.qualityProperties.metrics
                          ?.timeliness2?.double * 100
                      ).toFixed(2)
                    }}
                    %
                  </td>
                  <td>
                    {{
                      (
                        measurementAvg.real.qualityProperties.metrics?.accuracy1
                          ?.double * 100
                      ).toFixed(2)
                    }}
                    %
                  </td>
                  <td>
                    {{ measurementAvg.real.additional?.pastEventsCount?.int }}
                  </td>
                  <td>
                    {{
                      (
                        measurementAvg.real.additional?.pastEventsDuration
                          ?.long / 1000000000
                      ).toFixed(4)
                    }}
                  </td>
                  <td>
                    {{
                      measurementAvg.real.additional?.forecastedEventsCount?.int
                    }}
                  </td>
                  <td>
                    {{
                      (
                        measurementAvg.real.additional?.forecastedEventsDuration
                          ?.long / 1000000000
                      ).toFixed(4)
                    }}
                  </td>

                  <template v-if="debugRef">
                    <td
                        v-for="sensorId in registeredSensorIds"
                        :key="sensorId"
                        :class="{
                      'bg-yellow-50':
                        measurementAvg.real.events?.[sensorId]?.additional
                          ?.pastEventsOriginated?.boolean === true,
                      'bg-orange-50':
                        measurementAvg.real.events?.[sensorId]?.additional
                          ?.forecastedEventsOriginated?.boolean === true,
                    }"
                    >
                      {{
                        measurementAvg.real.events?.[sensorId]?.measurement?.value
                            ?.double
                      }}
                    </td>
                  </template>
                </tr>
              </tbody>
            </table>
          </div>

          <div class="bg-white p-2 text-xs">
            <pre>{{ lastMeasurementsAvg }}</pre>
          </div>
        </div>
      </div>
    </div>
  </main>
</template>

<style scoped>
.CommonSimpleTable {
  width: 100%;
  font-size: 16px;
  font-weight: 400;
  color: #05193c;
  border: 1px solid #d1d5db;
  border-collapse: collapse;
  table-layout: fixed;
}
.CommonSimpleTable--SmallFont {
  font-size: 13px;
}
.CommonSimpleTable th,
.CommonSimpleTable td {
  border: 1px solid #d1d5db;
}
.CommonSimpleTable th,
.CommonSimpleTable td {
  padding: 0.1em 0.3em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.CommonSimpleTable .VoidCell {
  background-color: #ffffff;
  background-image: url("data:image/svg+xml,%3Csvg width='6' height='6' viewBox='0 0 6 6' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='%23484848' fill-opacity='0.4' fill-rule='evenodd'%3E%3Cpath d='M5 0h1L0 6V5zM6 5v1H5z'/%3E%3C/g%3E%3C/svg%3E");
}
</style>
