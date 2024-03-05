<script setup>
import moment from "moment";
import { computed, ref, onMounted, onBeforeUnmount } from "vue";
import { DEMO_DATA } from "@/views/demo.js";

// const DEMO_DATA_DISPLAY = DEMO_DATA;
const DEMO_DATA_DISPLAY = DEMO_DATA.slice(15);

const formatDateTime = (timestamp) => {
  // timestamp = timestamp - (11 * 1000 * 60 * 60);
  const dt = new Date(timestamp);
  return moment(dt).format("HH:mm:ss");
};
</script>

<template>
  <main class="bg-gray-200 min-h-full">
    <header class="py-4 bg-black border-t border-t-gray-600">
      <div class="mx-auto px-4">
        <h1 class="text-xl font-bold tracking-tight text-white">Real-Time Monitoring <span class="text-sm">/ complex events</span></h1>
      </div>
    </header>

    <!-- mx-auto max-w-7xl -->
    <div class="mx-auto my-4 px-4">
      <!-- mx-auto max-w-5xl -->
      <div class="mx-auto">
        <div class="space-y-4">

          <div class="bg-white border border-gray-300">
            <div class="p-2">
              <p>Displaying <span class="bg-gray-200 px-2 py-1" style="font-family: monospace;">50</span> complex events created by <span class="bg-gray-200 px-2 py-1" style="font-family: monospace;" title="time window size of 5 secs with at least 4 sensors, past events lookup 6 windows, forecasted events lookup 6 windows of size 5 secs">w_avg_temperature_PT5S_null_null_4_false_6_PT5S_6</span> composite transformation</p>
            </div>
            <div class="flex space-x-2 p-2">
              <select class="border p-0.5 text-xs">
                <option>--- select composite transformation ---</option>
              </select>
              <button class="text-xs bg-gray-100 px-2 py-1 border border-gray-300">download spreadsheets (.zip)</button>
              <button class="text-xs bg-gray-100 px-2 py-1 border border-gray-300">download figures (.zip)</button>
            </div>
          </div>

          <div class="bg-white">
            <table class="CommonSimpleTable" style="text-align: center;">
              <thead>
              <tr>
                <th colspan="3">Time Window (simulation)</th>
                <th colspan="1">Air Temperature</th>
                <th colspan="4">Quality Properties</th>
                <th colspan="2">Events</th>
                <th colspan="2">Fabrication: Past Events</th>
                <th colspan="2">Fabrication: Forecasting</th>
                <!--
                <th colspan="6" v-if="debugRef">sensors</th>
                -->
              </tr>
              <tr>
                <th>Number</th>
                <th>Start</th>
                <th>End</th>

                <th>Average Value</th>

                <th colspan="2">Completeness</th>
                <th>Timeliness (1)</th>
                <th>Timeliness (2)</th>
                <th>Real</th>
                <th>Fabricated</th>
                <th>Count</th>
                <th>Duration</th>
                <th>Count</th>
                <th>Duration</th>

                <!-- sensors -->
                <!-- todo dynamically -->
                <!--
                <th v-if="debugRef">1</th>
                <th v-if="debugRef">2</th>
                <th v-if="debugRef">3</th>
                <th v-if="debugRef">4</th>
                <th v-if="debugRef">5</th>
                <th v-if="debugRef">6</th>
                -->
              </tr>
              </thead>
              <tbody>
              <tr
                  v-for="row in DEMO_DATA_DISPLAY"
              >
                <td>
                  {{
                    row.recurring_window
                  }}
                </td>
                <td>
                  {{ formatDateTime(row.start_timestamp) }}
                </td>
                <td>
                  {{ formatDateTime(row.end_timestamp) }}
                </td>
                <td :title="row.calculated_average">
                  {{ row.calculated_average.toFixed(2) }}
                </td>
                <template v-if="row.completeness2_before === 1.0">
                  <td colspan="2">    {{
                      (
                          row.completeness2_before * 100
                      ).toFixed(2)
                    }}
                    %</td>
                </template>
                <template v-else>
                  <td class="bg-red-50">
                    {{
                      (
                          row.completeness2_before * 100
                      ).toFixed(2)
                    }}
                    %
                  </td>
                  <td class="bg-green-50">
                    {{
                      (
                          row.completeness2 * 100
                      ).toFixed(2)
                    }}
                    %
                  </td>
                </template>
                <td>
                  {{
                    (
                        row.timeliness1 * 100
                    ).toFixed(2)
                  }}
                  %
                </td>
                <td>
                  {{
                    (
                        row.timeliness2 * 100
                    ).toFixed(2)
                  }}
                  %
                </td>
                <td>
                  {{ row.real_events_count }}
                </td>
                <td :class="{'bg-yellow-50': row.fabricated_events_count > 0}">
                  {{ row.fabricated_events_count }}
                </td>
                <td>
                  {{ row.past_events_count }}
                </td>
                <td>
                  {{
                    (
                        row.past_events_duration / 1000000000
                    ).toFixed(4)
                  }}
                </td>
                <td>
                  {{
                    row.forecasted_events_count
                  }}
                </td>
                <td>
                  {{
                    (
                        row.forecasted_events_duration / 1000000000
                    ).toFixed(4)
                  }}
                </td>

                <!--
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
                -->
              </tr>
              </tbody>
            </table>
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
