<script setup>
import { ref, onMounted, onBeforeUnmount } from "vue";

const measurements = ref([]);
const measurementsAvg = ref([]);

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
</script>

<template>
  <main class="bg-gray-200 min-h-full">
    <header class="py-6 bg-black border-t border-t-gray-600">
      <div class="mx-auto px-4">
        <h1 class="text-3xl font-bold tracking-tight text-white">Stream</h1>
      </div>
    </header>

    <div>
      <table>
        <thead></thead>
        <tbody>
          <tr
            v-for="measurementAvg in measurementsAvg"
            :key="measurementAvg.real.identifiers.clientSideId.string"
          >
            <td>{{ new Date(measurementAvg.real.startTimestamp) }}</td>
            <td>{{ new Date(measurementAvg.real.endTimestamp) }}</td>
            <td>{{ measurementAvg.real.average.name }}</td>
            <td>{{ measurementAvg.real.average.value.double }}</td>
            <td>
              {{ measurementAvg.real.qualityProperties.completeness.double }}
            </td>
            <td>
              {{ measurementAvg.real.qualityProperties.timeliness.double }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div>
      <ul id="data"></ul>
    </div>
  </main>
</template>
