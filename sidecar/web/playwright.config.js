// playwright.config.js — e2e test config for the dashboard.
//
// Plan reference: docs/plan_v0.8.5.md §7.5 CHECK-T20 (tooltip e2e).
// Runs against the Vite dev server on 5173. CI is expected to
// `npm run build` + serve dist/, but for local iteration this config
// starts the dev server on demand.

import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  fullyParallel: true,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  // Spin up Vite on demand so `npm run test:e2e` works standalone.
  webServer: {
    command: 'npm run dev',
    url: 'http://localhost:5173',
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
})
