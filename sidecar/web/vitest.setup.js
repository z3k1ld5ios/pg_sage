// vitest.setup.js — test environment bootstrap for Vitest.
//
// Loads @testing-library/jest-dom custom matchers (toBeInTheDocument,
// toHaveAttribute, ...). Run once before the test suite.
//
// jsdom does not implement ResizeObserver or DOMRect, both of which
// Radix uses internally for positioning. Polyfill with no-op stubs so
// the tooltip can render in tests without exercising layout math.

import '@testing-library/jest-dom/vitest'

class NoopResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver = globalThis.ResizeObserver || NoopResizeObserver

if (!globalThis.DOMRect) {
  globalThis.DOMRect = class DOMRect {
    constructor(x = 0, y = 0, width = 0, height = 0) {
      this.x = x
      this.y = y
      this.width = width
      this.height = height
      this.top = y
      this.left = x
      this.right = x + width
      this.bottom = y + height
    }
  }
}
