<script setup>
import { RouterLink, RouterView } from "vue-router";
</script>

<template>
  <div class="flex flex-col h-screen">
    <header
      class="relative flex h-8 px-4 space-x-4 flex-shrink-0 items-center bg-black"
    >
      <p class="text-white font-black">IoT VM DQ</p>
      <div class="space-x-4">
        <RouterLink to="/" class="text-gray-300 text-xs leading-loose"
          >Home</RouterLink
        >
        <RouterLink
          to="/dashboard-v1"
          class="text-gray-300 text-xs leading-loose"
          >Dashboard v1</RouterLink
        >
        <RouterLink to="/stream-v1" class="text-gray-300 text-xs leading-loose"
          >Stream v1</RouterLink
        >
      </div>
    </header>
    <main class="flex-1 overflow-y-auto">
      <RouterView />
    </main>
    <footer class="relative bg-black">
      <div class="text-center py-1 px-4">
        <p class="text-white text-xs">Powered by Software Force</p>
      </div>
    </footer>
  </div>
</template>
