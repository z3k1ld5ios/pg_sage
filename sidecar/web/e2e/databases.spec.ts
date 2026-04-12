// databases.spec.ts — Smoke tests for the Databases management page.
//
// Verifies that the admin-only database management page loads,
// displays the database table, and renders action buttons.

import { test, expect } from '@playwright/test'
import {
  mockAllAPIs,
  mockAuthMe,
  mockManagedDatabases,
} from './fixtures'

test.describe('Databases management page', () => {
  test.beforeEach(async ({ page }) => {
    await mockAllAPIs(page)
  })

  test('page loads for admin users', async ({ page }) => {
    await page.goto('#/manage-databases')

    // The "Add Database" button is the primary indicator the page loaded.
    await expect(
      page.locator('[data-testid="add-database-button"]'),
    ).toBeVisible()
    await expect(
      page.locator('[data-testid="add-database-button"]'),
    ).toContainText('Add Database')
  })

  test('import CSV button is visible', async ({ page }) => {
    await page.goto('#/manage-databases')

    await expect(
      page.locator('[data-testid="import-csv-button"]'),
    ).toBeVisible()
    await expect(
      page.locator('[data-testid="import-csv-button"]'),
    ).toContainText('Import CSV')
  })

  test('database table renders with managed databases', async ({
    page,
  }) => {
    await page.goto('#/manage-databases')

    const table = page.locator('[data-testid="databases-table"]')
    await expect(table).toBeVisible()

    // Table headers should be present.
    await expect(table).toContainText('Name')
    await expect(table).toContainText('Host')
    await expect(table).toContainText('Trust Level')
    await expect(table).toContainText('Execution Mode')

    // Rows should match our mock data.
    const rows = page.locator('[data-testid="db-row"]')
    await expect(rows).toHaveCount(
      mockManagedDatabases.databases.length,
    )
  })

  test('database rows display correct data', async ({ page }) => {
    await page.goto('#/manage-databases')

    const rows = page.locator('[data-testid="db-row"]')

    // First row: "primary" database.
    const firstRow = rows.first()
    await expect(firstRow).toContainText('primary')
    await expect(firstRow).toContainText('db.example.com')
    await expect(firstRow).toContainText('advisory')
    await expect(firstRow).toContainText('approval')

    // Second row: "replica" database.
    const secondRow = rows.nth(1)
    await expect(secondRow).toContainText('replica')
    await expect(secondRow).toContainText('observation')
  })

  test('each row has test, edit, and delete buttons', async ({
    page,
  }) => {
    await page.goto('#/manage-databases')

    const firstRow = page.locator('[data-testid="db-row"]').first()

    await expect(
      firstRow.locator('[data-testid="db-test-button"]'),
    ).toBeVisible()
    await expect(
      firstRow.locator('[data-testid="db-edit-button"]'),
    ).toBeVisible()
    await expect(
      firstRow.locator('[data-testid="db-delete-button"]'),
    ).toBeVisible()
  })

  test('non-admin user is redirected to dashboard', async ({
    page,
  }) => {
    // Override auth to return a non-admin user.
    await page.route('**/api/v1/auth/me', route =>
      route.fulfill({
        json: { ...mockAuthMe, role: 'viewer' },
      }),
    )

    await page.goto('#/manage-databases')

    // App routes non-admins to Dashboard for /manage-databases.
    // The health-hero (dashboard) or onboarding should appear
    // instead of the database management table.
    await expect(
      page.locator('[data-testid="add-database-button"]'),
    ).not.toBeVisible()
  })

  test('clicking add database opens the form', async ({ page }) => {
    await page.goto('#/manage-databases')

    await page.locator(
      '[data-testid="add-database-button"]',
    ).click()

    // The DatabaseForm should appear — it has a form with input fields.
    await expect(
      page.locator('[data-testid="db-form"]'),
    ).toBeVisible()
  })
})
