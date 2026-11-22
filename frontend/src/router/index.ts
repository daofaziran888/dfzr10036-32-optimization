import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import HandoffWorkbench from '@/views/HandoffWorkbench.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/handoff-workbench', name: 'handoff-workbench', component: HandoffWorkbench },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
