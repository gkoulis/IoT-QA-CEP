<script setup>
import moment from "moment";
import { ref, onMounted, onBeforeUnmount } from "vue";

const measurements = ref([]);
const measurementsAvg = ref([]);
const lastMeasurementsAvg = ref(null);

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
      if (
        eventData.topicName ===
        "ga.sensor_telemetry_measurements_average_event.0001"
      ) {
        measurementsAvg.value.push(eventData);
        lastMeasurementsAvg.value = eventData;
      } else if (
        eventData.topicName === "ga.sensor_telemetry_measurement_event.0001"
      ) {
        measurements.value.push(eventData);
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

    <div class="my-4 mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
      <div class="mx-auto max-w-5xl">
        <div class="space-y-4">
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

          <div class="bg-white">
            <table class="CommonSimpleTable">
              <thead>
                <tr>
                  <th colspan="2">time window</th>
                  <th colspan="2">average</th>
                  <th colspan="3">quality properties</th>
                  <th colspan="2">past events</th>
                  <th colspan="2">forecasted events</th>
                </tr>
                <tr>
                  <th>start</th>
                  <th>end</th>
                  <th>name</th>
                  <th>value</th>
                  <th>completeness</th>
                  <th>timeliness</th>
                  <th>timeliness (alt)</th>
                  <th>count</th>
                  <th>duration</th>
                  <th>count</th>
                  <th>duration</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="measurementAvg in measurementsAvg"
                  :key="measurementAvg.real.identifiers.clientSideId.string"
                >
                  <td>
                    {{ formatDateTime(measurementAvg.real.startTimestamp) }}
                  </td>
                  <td>
                    {{ formatDateTime(measurementAvg.real.endTimestamp) }}
                  </td>
                  <td>{{ measurementAvg.real.average.name }}</td>
                  <td>
                    {{ measurementAvg.real.average.value.double.toFixed(2) }}
                  </td>
                  <td>
                    {{
                      measurementAvg.real.qualityProperties.completeness
                        .double * 100
                    }}
                    %
                  </td>
                  <td>
                    {{
                      measurementAvg.real.qualityProperties.timeliness.double *
                      100
                    }}
                    %
                  </td>
                  <td>
                    {{
                      measurementAvg.real.additional?.timelinessAlt?.double *
                      100
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

<style>
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
