import { createApp } from "vue";
import { createPinia } from "pinia";
import PrimeVue from "primevue/config";
import Chart from "primevue/chart";

import "./index.css";

// import "primevue/resources/themes/saga-blue/theme.css";
import "primevue/resources/themes/fluent-light/theme.css";
import "primevue/resources/primevue.min.css";
import "primeicons/primeicons.css";

import "./index_overrides.css";

import App from "./App.vue";
import router from "./router";

const app = createApp(App);

app.use(createPinia());
app.use(router);
app.use(PrimeVue);

app.mount("#app");

app.component("Chart", Chart);
