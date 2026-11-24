<script lang="ts">
// Shared across instances so each render gets a unique element id.
let nextId = 0
</script>

<script setup lang="ts">
// Renders a ```mermaid fence in the browser. The server-rendered page and
// readers without JavaScript see the diagram source instead.
import { computed, onMounted, ref, watch } from 'vue'
import { useData } from 'vitepress'

const props = defineProps<{ code: string }>()
const { isDark } = useData()
const source = computed(() => decodeURIComponent(props.code))
const svg = ref('')

let generation = 0

async function render() {
  const current = ++generation
  const { default: mermaid } = await import('mermaid')
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: 'strict',
    theme: isDark.value ? 'dark' : 'default'
  })
  try {
    const { svg: rendered } = await mermaid.render(`mermaid-${nextId++}`, source.value)
    if (current === generation) {
      svg.value = rendered
    }
  } catch (error) {
    console.error('Mermaid diagram failed to render', error)
    if (current === generation) {
      svg.value = ''
    }
  }
}

onMounted(render)
watch([isDark, source], render)
</script>

<template>
  <div v-if="svg" class="mermaid-diagram" v-html="svg" />
  <div v-else class="language-mermaid"><pre><code>{{ source }}</code></pre></div>
</template>

<style scoped>
.mermaid-diagram {
  margin: 16px 0;
  overflow-x: auto;
  text-align: center;
}
</style>
