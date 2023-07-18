<script setup>
import { ref } from "vue";

const message = ref("Not yet implemented");
</script>

<template>
  <main class="bg-gray-200 min-h-full">
    <div class="p-4 space-y-4 text-center">
      <p>{{ message }}</p>
    </div>
  </main>
</template>
