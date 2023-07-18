<script setup>
import { ref, onMounted } from "vue";

onMounted(() => {
  lineChartData.value = setLineChartData();
  lineChartOptions.value = setLineChartOptions();
  radarChartData.value = setRadarChartData();
  radarChartOptions.value = setRadarChartOptions();
});

const lineChartData = ref();
const lineChartOptions = ref();
const radarChartData = ref();
const radarChartOptions = ref();

const setLineChartData = () => {
  const documentStyle = getComputedStyle(document.documentElement);

  return {
    labels: ["t1", "t2", "t3", "t4", "t5", "t6", "t7"],
    datasets: [
      {
        label: "Sensor 1",
        data: [39, 38, 40, 40.1, 42, 40.9, 40],
        fill: false,
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--red-500"),
      },
      {
        label: "Sensor 2",
        data: [43, 43, 39, 43, 39, 41, 42.5],
        fill: false,
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--green-500"),
      },
      {
        label: "Sensor 3",
        data: [39, 39, 39, 39.5, 42, 39.5, 43],
        fill: false,
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--blue-500"),
      },
      {
        label: "Sensor 4",
        data: [42, 41.5, 41.8, 43, 42, 41, 39],
        fill: false,
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--teal-500"),
      },
      {
        label: "Sensor 5",
        data: [39.1, 40, 39.8, 40.1, 41, 39.1, 39.6],
        fill: false,
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--cyan-500"),
      },
      {
        label: "Sensor 6",
        data: [40, 41, 40.8, 40.7, 41, 41.2, 41.1],
        fill: false,
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--orange-500"),
      },
      {
        label: "avg",
        data: [40.5, 40.6, 41, 40, 40.5, 40.2, 40.8],
        fill: false,
        borderDash: [5, 5],
        tension: 0.4,
        borderColor: documentStyle.getPropertyValue("--black"),
      },
    ],
  };
};
const setLineChartOptions = () => {
  const documentStyle = getComputedStyle(document.documentElement);
  const textColor = documentStyle.getPropertyValue("--text-color");
  const textColorSecondary = documentStyle.getPropertyValue(
    "--text-color-secondary"
  );
  const surfaceBorder = documentStyle.getPropertyValue("--surface-border");

  return {
    maintainAspectRatio: false,
    aspectRatio: 0.6,
    plugins: {
      legend: {
        labels: {
          color: textColor,
        },
      },
    },
    // TODO min-max (chart boundaries)
    scales: {
      x: {
        ticks: {
          color: textColorSecondary,
        },
        grid: {
          color: surfaceBorder,
        },
      },
      y: {
        ticks: {
          color: textColorSecondary,
        },
        grid: {
          color: surfaceBorder,
        },
      },
    },
  };
};

const setRadarChartData = () => {
  const documentStyle = getComputedStyle(document.documentElement);
  const textColor = documentStyle.getPropertyValue("--text-color");

  return {
    labels: ["Accuracy", "Completeness", "Timeliness", "Inclusion", "Validity"],
    datasets: [
      {
        label: "Quality Properties",
        borderColor: documentStyle.getPropertyValue("--blue-400"),
        pointBackgroundColor: documentStyle.getPropertyValue("--blue-400"),
        pointBorderColor: documentStyle.getPropertyValue("--blue-400"),
        pointHoverBackgroundColor: textColor,
        pointHoverBorderColor: documentStyle.getPropertyValue("--blue-400"),
        data: [0.9, 1, 1, 0.8, 1],
      },
      {
        label: "Penalties",
        borderColor: documentStyle.getPropertyValue("--pink-400"),
        pointBackgroundColor: documentStyle.getPropertyValue("--pink-400"),
        pointBorderColor: documentStyle.getPropertyValue("--pink-400"),
        pointHoverBackgroundColor: textColor,
        pointHoverBorderColor: documentStyle.getPropertyValue("--pink-400"),
        data: [0.1, 0, 0, 0.2, 0],
      },
      {
        label: "Penalties by Completeness violations",
        borderColor: documentStyle.getPropertyValue("--pink-200"),
        pointBackgroundColor: documentStyle.getPropertyValue("--pink-200"),
        pointBorderColor: documentStyle.getPropertyValue("--pink-200"),
        pointHoverBackgroundColor: textColor,
        pointHoverBorderColor: documentStyle.getPropertyValue("--pink-200"),
        data: [0.11, 0.17, 0.11, 0.12, 0.05],
      },
    ],
  };
};
const setRadarChartOptions = () => {
  const documentStyle = getComputedStyle(document.documentElement);
  const textColor = documentStyle.getPropertyValue("--text-color");
  const textColorSecondary = documentStyle.getPropertyValue(
    "--text-color-secondary"
  );

  return {
    plugins: {
      legend: {
        labels: {
          color: textColor,
        },
      },
    },
    scales: {
      r: {
        suggestedMin: 0,
        suggestedMax: 1,
        grid: {
          color: textColorSecondary,
        },
      },
    },
  };
};
</script>

<template>
  <main class="bg-gray-200 min-h-full">
    <header class="py-6 bg-black border-t border-t-gray-600">
      <div class="mx-auto px-4">
        <h1 class="text-3xl font-bold tracking-tight text-white">Dashboard</h1>
      </div>
    </header>

    <!-- Day Selection -->
    <div class="py-2 bg-gray-700">
      <div class="mx-auto px-4 flex justify-between content-center">
        <p class="tracking-tight text-white">
          Day 2023-01-01
          <small class="text-xs"
            >from <span class="underline">2023-01-01 00:00:00</span> to
            <span class="underline">2023-01-01 23:59:59</span></small
          >
        </p>
        <p class="tracking-tight text-white text-sm flex space-x-1">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
            stroke-width="1.5"
            stroke="currentColor"
            class="w-6 h-6 stroke-green-300"
          >
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z"
            />
          </svg>
          <span>current day - receiving data</span>
        </p>
      </div>
    </div>

    <div>
      <dl
        class="mx-auto grid grid-cols-1 gap-px bg-gray-900/5 sm:grid-cols-2 lg:grid-cols-4"
      >
        <div
          class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 bg-white px-4 py-10"
        >
          <dt class="text-sm font-medium leading-6 text-gray-500">
            Temperature
          </dt>
          <dd
            class="w-full flex-none text-6xl font-medium leading-10 tracking-tight text-gray-900"
          >
            25,2 °C
          </dd>
          <table class="min-w-full divide-y divide-gray-300">
            <tbody class="divide-y divide-gray-200">
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800">min temperature</p>
                  <p class="text-xs text-gray-300">@ 2023-01-01 04:00:00</p>
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800">9,1 °C</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800">max temperature</p>
                  <p class="text-xs text-gray-300">@ 2023-01-01 15:00:00</p>
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800">50,1 °C</td>
              </tr>
            </tbody>
          </table>
          <dt class="text-xs leading-1 text-gray-500">
            Temperature is calculated by averaging the last measurements of each
            IoT device for the predetermined time window.
          </dt>
        </div>
        <div
          class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 bg-white px-4 py-10 sm:px-6 xl:px-8"
        >
          <p class="text-sm font-medium leading-6 text-gray-500">
            Quality Properties
          </p>
          <table class="min-w-full divide-y divide-gray-300">
            <tbody class="divide-y divide-gray-200">
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800">Accuracy</p>
                  <p class="text-xs text-gray-300">accuracy1</p>
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800">1</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800">Completeness</p>
                  <p class="text-xs text-gray-300">completeness1</p>
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800">1</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800">Timeliness</p>
                  <p class="text-xs text-gray-300">timeliness1</p>
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800">1</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800">Inclusion</p>
                  <p class="text-xs text-gray-300">inclusion1</p>
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800">1</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div
          class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 bg-white px-4 py-10 sm:px-6 xl:px-8"
        >
          <dt class="text-sm font-medium leading-6 text-gray-500">
            Diagnostics
          </dt>
          <dd class="text-xs font-medium text-green-600">
            all sensors are live
          </dd>
          <table class="min-w-full divide-y divide-gray-300">
            <tbody class="divide-y divide-gray-200">
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  last event
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">
                  2023-01-01 15:59:30
                </td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  soft sensing enabled
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">
                  Yes
                </td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  soft sensing activated
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">No</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  fabricated events
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">0</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  late events
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">0</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  observations
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">15</td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2 text-gray-700 text-sm">
                  processed events
                </td>
                <td class="whitespace-nowrap py-2 text-gray-800 text-sm">15</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div
          class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 bg-white px-4 py-10 sm:px-6 xl:px-8"
        >
          <dt class="text-sm font-medium leading-6 text-gray-500">
            Time Window
          </dt>
          <dd class="text-xs font-medium text-blue-600">
            Hopping Time Window (1m, 5m)
          </dd>
          <table class="min-w-full divide-y divide-gray-300">
            <tbody class="divide-y divide-gray-200">
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-300 text-sm">1</p>
                </td>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-800 font-semibold text-sm">
                    2023-01-01 15:59:00 - 2023-01-01 15:59:59
                  </p>
                </td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-300 text-sm">2</p>
                </td>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-700 text-sm">
                    2023-01-01 15:58:00 - 2023-01-01 15:58:59
                  </p>
                </td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-300 text-sm">3</p>
                </td>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-700 text-sm">
                    2023-01-01 15:57:00 - 2023-01-01 15:57:59
                  </p>
                </td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-300 text-sm">4</p>
                </td>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-700 text-sm">
                    2023-01-01 15:56:00 - 2023-01-01 15:56:59
                  </p>
                </td>
              </tr>
              <tr>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-300 text-sm">5</p>
                </td>
                <td class="whitespace-nowrap py-2">
                  <p class="text-gray-700 text-sm">
                    2023-01-01 15:55:00 - 2023-01-01 15:55:59
                  </p>
                </td>
              </tr>
            </tbody>
          </table>
          <dd class="text-xs text-gray-500">
            enabled update strategies:
            <ul class="list-disc">
              <li>on window update</li>
              <li>on window close</li>
            </ul>
          </dd>
        </div>
      </dl>
    </div>

    <!-- Charts -->
    <div class="border-t border-gray-900/5">
      <div class="mx-auto grid grid-cols-1 gap-px bg-gray-900/5 sm:grid-cols-2">
        <div
          class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 bg-white px-4 py-10"
        >
          <Chart
            type="line"
            :data="lineChartData"
            :options="lineChartOptions"
            style="width: 100%; height: 800px"
          />
        </div>
        <div
          class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-2 bg-white px-4 py-10"
        >
          <Chart
            type="radar"
            :data="radarChartData"
            :options="radarChartOptions"
            style="width: 100%"
          />
        </div>
      </div>
    </div>
  </main>
</template>
