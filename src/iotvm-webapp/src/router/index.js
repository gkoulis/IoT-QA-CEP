import { createRouter, createWebHistory } from "vue-router";
import HomeView from "../views/HomeView.vue";

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: "/",
      name: "home",
      component: HomeView,
    },
    {
      path: "/dashboard-v1",
      name: "dashboard-v1",
      component: () => import("../views/DashboardV1View.vue"),
    },
    {
      path: "/stream-v1",
      name: "stream-v1",
      component: () => import("../views/StreamV1View.vue"),
    },
  ],
});

export default router;
