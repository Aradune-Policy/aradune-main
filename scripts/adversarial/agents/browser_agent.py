"""
Browser Adversarial Agent

Uses Playwright to run browser-based UI tests against the Aradune frontend:
- Homepage and navigation rendering
- JS error detection across all routes
- Mobile viewport responsiveness
- Rapid state filter switching (stress test)
- Intelligence query submission and SSE streaming
- Cmd+K platform search
- Export button resilience during page load

Requires: pip install playwright && playwright install chromium
"""

import os
import time
import logging

from scripts.adversarial.config import FRONTEND_BASE

logger = logging.getLogger("adversarial.browser")

# Password for the client-side gate (not a security boundary)
PASSWORD_GATE = "mediquiad"

# Routes to test for JS errors (hash-based routing)
NAV_ROUTES = [
    "/",
    "/#/state/FL",
    "/#/rates",
    "/#/cpra",
    "/#/forecast",
    "/#/providers",
    "/#/about",
]

# States for rapid switching test
RAPID_SWITCH_STATES = ["GA", "TX", "CA", "NY", "OH", "PA"]


class BrowserAgent:
    """Adversarial agent: Playwright UI tests for Aradune frontend."""

    def __init__(self):
        self.results = []

    def run(self) -> dict:
        """Run all browser tests, return standard agent report."""
        t0 = time.time()

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return {
                "agent": "browser",
                "passed": 0,
                "total": 0,
                "pass_rate": 0,
                "duration_s": 0,
                "results": [],
                "error": "Playwright not installed. Run: pip install playwright && playwright install chromium",
            }

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()

            for test_fn in [
                self._test_homepage_loads,
                self._test_no_js_errors_navigation,
                self._test_mobile_viewport,
                self._test_rapid_state_switching,
                self._test_intelligence_query_renders,
                self._test_sse_streaming_completes,
                self._test_cmd_k_search,
                self._test_export_during_load,
            ]:
                try:
                    test_fn(context)
                except Exception as e:
                    self.results.append({
                        "test": test_fn.__name__,
                        "passed": False,
                        "reason": f"Exception: {str(e)[:200]}",
                    })

            browser.close()

        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)

        return {
            "agent": "browser",
            "passed": passed,
            "total": total,
            "pass_rate": round(passed / max(total, 1) * 100, 1),
            "duration_s": round(time.time() - t0, 1),
            "results": self.results,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _pass_password_gate(self, page):
        """If the password gate is showing, enter the password and submit."""
        try:
            pw_input = page.locator("input[type='password']")
            if pw_input.count() > 0 and pw_input.first.is_visible(timeout=2000):
                pw_input.first.fill(PASSWORD_GATE)
                # Try clicking a submit button, or pressing Enter
                submit_btn = page.locator("button[type='submit'], button:has-text('Enter'), button:has-text('Submit')")
                if submit_btn.count() > 0:
                    submit_btn.first.click()
                else:
                    pw_input.first.press("Enter")
                # Wait for gate to clear
                page.wait_for_timeout(2000)
        except Exception:
            # Gate may not be present; that is fine
            pass

    def _collect_console_errors(self, page):
        """Attach a console listener and return a mutable list that accumulates errors."""
        errors = []

        def on_console(msg):
            if msg.type == "error":
                text = msg.text
                # Ignore benign browser/extension noise
                benign = [
                    "favicon.ico",
                    "net::ERR_",
                    "Failed to load resource",
                    "third-party",
                    "ResizeObserver",
                ]
                if not any(b in text for b in benign):
                    errors.append(text)

        page.on("console", on_console)
        return errors

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def _test_homepage_loads(self, context):
        """Navigate to /, verify 'Aradune' text is present, capture JS errors."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(FRONTEND_BASE, timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)

            # Wait for content to render
            page.wait_for_timeout(2000)

            body_text = page.text_content("body") or ""
            has_aradune = "Aradune" in body_text or "aradune" in body_text.lower()

            if not has_aradune:
                self.results.append({
                    "test": "test_homepage_loads",
                    "passed": False,
                    "reason": "'Aradune' text not found on homepage",
                    "js_errors": errors[:5],
                })
            elif errors:
                self.results.append({
                    "test": "test_homepage_loads",
                    "passed": False,
                    "reason": f"{len(errors)} JS console error(s) on homepage",
                    "js_errors": errors[:5],
                })
            else:
                self.results.append({
                    "test": "test_homepage_loads",
                    "passed": True,
                    "reason": "Homepage loaded with 'Aradune' text, no JS errors",
                })
        finally:
            page.close()

    def _test_no_js_errors_navigation(self, context):
        """Navigate to 7 routes, collect JS errors. Pass = zero errors."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            # Initial load + password gate
            page.goto(FRONTEND_BASE, timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)

            route_errors = {}

            for route in NAV_ROUTES:
                url = f"{FRONTEND_BASE}{route}"
                pre_count = len(errors)
                page.goto(url, timeout=10000, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
                new_errors = errors[pre_count:]
                if new_errors:
                    route_errors[route] = new_errors[:3]

            total_errors = len(errors)

            if total_errors == 0:
                self.results.append({
                    "test": "test_no_js_errors_navigation",
                    "passed": True,
                    "reason": f"Navigated {len(NAV_ROUTES)} routes with zero JS errors",
                })
            else:
                self.results.append({
                    "test": "test_no_js_errors_navigation",
                    "passed": False,
                    "reason": f"{total_errors} JS error(s) across {len(route_errors)} route(s)",
                    "route_errors": route_errors,
                })
        finally:
            page.close()

    def _test_mobile_viewport(self, context):
        """iPhone viewport (390x844): no horizontal overflow, no unwrapped tables."""
        # Create a separate context with mobile viewport
        from playwright.sync_api import sync_playwright

        browser = context.browser
        mobile_context = browser.new_context(
            viewport={"width": 390, "height": 844},
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
        )
        page = mobile_context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(f"{FRONTEND_BASE}/#/state/FL", timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)
            page.wait_for_timeout(3000)

            # Check for horizontal overflow
            overflow_check = page.evaluate("""() => {
                const body = document.body;
                const html = document.documentElement;
                const pageWidth = Math.max(
                    body.scrollWidth, body.offsetWidth,
                    html.clientWidth, html.scrollWidth, html.offsetWidth
                );
                const viewportWidth = window.innerWidth;
                return {
                    pageWidth: pageWidth,
                    viewportWidth: viewportWidth,
                    hasOverflow: pageWidth > viewportWidth + 5,
                };
            }""")

            # Check for tables without overflow wrapper
            unwrapped_tables = page.evaluate("""() => {
                const tables = document.querySelectorAll('table');
                let unwrapped = 0;
                for (const table of tables) {
                    const parent = table.parentElement;
                    if (parent) {
                        const parentStyle = window.getComputedStyle(parent);
                        const hasOverflow = parentStyle.overflowX === 'auto'
                            || parentStyle.overflowX === 'scroll'
                            || parentStyle.overflow === 'auto'
                            || parentStyle.overflow === 'scroll';
                        if (!hasOverflow && table.scrollWidth > 390) {
                            unwrapped++;
                        }
                    }
                }
                return { total: tables.length, unwrapped: unwrapped };
            }""")

            issues = []
            if overflow_check.get("hasOverflow"):
                issues.append(
                    f"Horizontal overflow: page width {overflow_check['pageWidth']}px "
                    f"> viewport {overflow_check['viewportWidth']}px"
                )
            if unwrapped_tables.get("unwrapped", 0) > 0:
                issues.append(
                    f"{unwrapped_tables['unwrapped']}/{unwrapped_tables['total']} "
                    f"tables lack overflow wrapper"
                )

            if issues:
                self.results.append({
                    "test": "test_mobile_viewport",
                    "passed": False,
                    "reason": "; ".join(issues),
                })
            else:
                self.results.append({
                    "test": "test_mobile_viewport",
                    "passed": True,
                    "reason": (
                        f"Mobile viewport OK. Page width {overflow_check['pageWidth']}px, "
                        f"{unwrapped_tables['total']} tables all properly wrapped"
                    ),
                })
        finally:
            page.close()
            mobile_context.close()

    def _test_rapid_state_switching(self, context):
        """Rapidly switch state filter: FL -> GA -> TX -> CA -> NY -> OH -> PA. No crashes."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(f"{FRONTEND_BASE}/#/state/FL", timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)
            page.wait_for_timeout(2000)

            crash_detected = False

            for state in RAPID_SWITCH_STATES:
                page.goto(
                    f"{FRONTEND_BASE}/#/state/{state}",
                    timeout=10000,
                    wait_until="domcontentloaded",
                )
                # Brief pause to let React re-render
                page.wait_for_timeout(500)

                # Check for React error boundary or crash overlay
                crash_indicators = page.locator(
                    "text='Something went wrong', "
                    "text='Uncaught Error', "
                    "text='ChunkLoadError', "
                    "[id='react-error-overlay']"
                )
                if crash_indicators.count() > 0:
                    crash_detected = True
                    break

            # Check for React-specific errors in console
            react_errors = [e for e in errors if any(kw in e for kw in [
                "React", "Uncaught", "Cannot read prop", "undefined is not",
                "TypeError", "ReferenceError", "ChunkLoadError",
            ])]

            if crash_detected:
                self.results.append({
                    "test": "test_rapid_state_switching",
                    "passed": False,
                    "reason": "React crash/error boundary triggered during rapid state switching",
                })
            elif react_errors:
                self.results.append({
                    "test": "test_rapid_state_switching",
                    "passed": False,
                    "reason": f"{len(react_errors)} React/JS error(s) during rapid switching",
                    "js_errors": react_errors[:5],
                })
            else:
                self.results.append({
                    "test": "test_rapid_state_switching",
                    "passed": True,
                    "reason": f"Rapid state switching through {len(RAPID_SWITCH_STATES)} states with no crashes",
                })
        finally:
            page.close()

    def _test_intelligence_query_renders(self, context):
        """Submit a query to Intelligence, wait for response text >50 chars."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(FRONTEND_BASE, timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)
            page.wait_for_timeout(2000)

            # Find the chat input -- try multiple selectors
            chat_input = page.locator(
                "textarea, "
                "input[placeholder*='Ask'], "
                "input[placeholder*='ask'], "
                "input[placeholder*='question'], "
                "input[placeholder*='Query'], "
                "[contenteditable='true']"
            )

            if chat_input.count() == 0:
                self.results.append({
                    "test": "test_intelligence_query_renders",
                    "passed": False,
                    "reason": "Could not find chat input on homepage",
                })
                return

            chat_input.first.fill("What is Florida's FMAP?")

            # Submit via Enter
            chat_input.first.press("Enter")

            # Wait for a response container with >50 chars of text
            # The response may appear in various containers
            response_found = False
            start_wait = time.time()
            timeout_s = 55

            while time.time() - start_wait < timeout_s:
                # Look for response text in common containers
                candidates = page.locator(
                    "[class*='response'], "
                    "[class*='message'], "
                    "[class*='answer'], "
                    "[class*='assistant'], "
                    "[class*='chat'] p, "
                    "[class*='chat'] div"
                )

                for i in range(min(candidates.count(), 20)):
                    try:
                        text = candidates.nth(i).text_content() or ""
                        # Filter out the user's own message and UI chrome
                        if len(text) > 50 and "Florida" not in text[:30]:
                            response_found = True
                            break
                    except Exception:
                        continue

                if response_found:
                    break

                page.wait_for_timeout(1000)

            if response_found:
                self.results.append({
                    "test": "test_intelligence_query_renders",
                    "passed": True,
                    "reason": f"Intelligence response rendered ({round(time.time() - start_wait, 1)}s)",
                })
            else:
                self.results.append({
                    "test": "test_intelligence_query_renders",
                    "passed": False,
                    "reason": f"No response >50 chars within {timeout_s}s",
                    "js_errors": errors[:3],
                })
        finally:
            page.close()

    def _test_sse_streaming_completes(self, context):
        """Submit a query, wait for SSE stream to complete (progress indicator or final content)."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(FRONTEND_BASE, timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)
            page.wait_for_timeout(2000)

            # Find chat input
            chat_input = page.locator(
                "textarea, "
                "input[placeholder*='Ask'], "
                "input[placeholder*='ask'], "
                "input[placeholder*='question'], "
                "[contenteditable='true']"
            )

            if chat_input.count() == 0:
                self.results.append({
                    "test": "test_sse_streaming_completes",
                    "passed": False,
                    "reason": "Could not find chat input",
                })
                return

            chat_input.first.fill("How many states expanded Medicaid?")
            chat_input.first.press("Enter")

            # Wait for streaming to complete
            # Look for: "Complete" indicator, response with substantial text,
            # or absence of loading/streaming indicators
            stream_complete = False
            start_wait = time.time()
            timeout_s = 55

            while time.time() - start_wait < timeout_s:
                page_text = page.text_content("body") or ""

                # Check for completion indicators
                complete_indicators = [
                    "Complete" in page_text,
                    "Sources" in page_text,  # citations section often marks completion
                ]

                # Check for substantial response content (stream done)
                response_containers = page.locator(
                    "[class*='response'], "
                    "[class*='message'], "
                    "[class*='assistant'], "
                    "[class*='answer']"
                )

                has_content = False
                for i in range(min(response_containers.count(), 10)):
                    try:
                        text = response_containers.nth(i).text_content() or ""
                        if len(text) > 100:
                            has_content = True
                            break
                    except Exception:
                        continue

                # Check that streaming/loading indicators are gone
                loading = page.locator(
                    "[class*='loading'], "
                    "[class*='spinner'], "
                    "[class*='streaming']"
                )
                still_loading = False
                try:
                    still_loading = loading.count() > 0 and loading.first.is_visible(timeout=500)
                except Exception:
                    pass

                if has_content and (any(complete_indicators) or not still_loading):
                    stream_complete = True
                    break

                page.wait_for_timeout(1000)

            if stream_complete:
                self.results.append({
                    "test": "test_sse_streaming_completes",
                    "passed": True,
                    "reason": f"SSE streaming completed ({round(time.time() - start_wait, 1)}s)",
                })
            else:
                self.results.append({
                    "test": "test_sse_streaming_completes",
                    "passed": False,
                    "reason": f"SSE stream did not complete within {timeout_s}s",
                    "js_errors": errors[:3],
                })
        finally:
            page.close()

    def _test_cmd_k_search(self, context):
        """Press Cmd+K (Mac) or Ctrl+K, verify search modal appears, type a query, verify results."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(FRONTEND_BASE, timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)
            page.wait_for_timeout(2000)

            # Press Meta+K (Mac) -- also try Ctrl+K as fallback
            page.keyboard.press("Meta+k")
            page.wait_for_timeout(1000)

            # Look for search modal/overlay
            search_modal = page.locator(
                "[class*='search'], "
                "[class*='Search'], "
                "[role='dialog'], "
                "[class*='modal'], "
                "[class*='overlay'], "
                "[class*='command']"
            )

            modal_visible = False
            for i in range(min(search_modal.count(), 10)):
                try:
                    if search_modal.nth(i).is_visible(timeout=1000):
                        modal_visible = True
                        break
                except Exception:
                    continue

            if not modal_visible:
                # Try Ctrl+K as fallback (non-Mac or alternate binding)
                page.keyboard.press("Control+k")
                page.wait_for_timeout(1000)

                for i in range(min(search_modal.count(), 10)):
                    try:
                        if search_modal.nth(i).is_visible(timeout=1000):
                            modal_visible = True
                            break
                    except Exception:
                        continue

            if not modal_visible:
                self.results.append({
                    "test": "test_cmd_k_search",
                    "passed": False,
                    "reason": "Search modal did not appear after Cmd+K / Ctrl+K",
                })
                return

            # Type a search query
            # The modal should have a focused input
            page.keyboard.type("Florida enrollment", delay=50)
            page.wait_for_timeout(2000)

            # Check for results
            results_found = False
            result_candidates = page.locator(
                "[class*='result'], "
                "[class*='Result'], "
                "[class*='item'], "
                "[class*='suggestion'], "
                "[class*='option'], "
                "[role='option'], "
                "[role='listbox'] > *"
            )

            if result_candidates.count() > 0:
                for i in range(min(result_candidates.count(), 20)):
                    try:
                        if result_candidates.nth(i).is_visible(timeout=500):
                            results_found = True
                            break
                    except Exception:
                        continue

            if results_found:
                self.results.append({
                    "test": "test_cmd_k_search",
                    "passed": True,
                    "reason": "Cmd+K search modal opened, query typed, results appeared",
                })
            else:
                # Modal opened but no results -- partial pass, still mark as fail
                self.results.append({
                    "test": "test_cmd_k_search",
                    "passed": False,
                    "reason": "Search modal opened but no results appeared for 'Florida enrollment'",
                })
        finally:
            page.close()

    def _test_export_during_load(self, context):
        """Navigate to /#/rates, click export/download during load, verify no JS crash."""
        page = context.new_page()
        errors = self._collect_console_errors(page)

        try:
            page.goto(f"{FRONTEND_BASE}/#/rates", timeout=10000, wait_until="domcontentloaded")
            self._pass_password_gate(page)

            # Don't wait for full load -- click export immediately (adversarial)
            page.wait_for_timeout(1000)

            # Find any export/download button
            export_btn = page.locator(
                "button:has-text('Export'), "
                "button:has-text('export'), "
                "button:has-text('Download'), "
                "button:has-text('download'), "
                "button:has-text('CSV'), "
                "button:has-text('Excel'), "
                "button:has-text('PDF'), "
                "a[download], "
                "[aria-label*='export'], "
                "[aria-label*='Export'], "
                "[aria-label*='download'], "
                "[aria-label*='Download']"
            )

            if export_btn.count() == 0:
                # No export button found yet -- wait a bit more
                page.wait_for_timeout(3000)

            clicked = False
            if export_btn.count() > 0:
                for i in range(min(export_btn.count(), 5)):
                    try:
                        if export_btn.nth(i).is_visible(timeout=1000):
                            export_btn.nth(i).click(timeout=3000)
                            clicked = True
                            break
                    except Exception:
                        continue

            # Wait and check for crash
            page.wait_for_timeout(2000)

            # Check for crash indicators
            crash_indicators = page.locator(
                "text='Something went wrong', "
                "text='Uncaught Error', "
                "[id='react-error-overlay']"
            )
            has_crash = crash_indicators.count() > 0

            react_errors = [e for e in errors if any(kw in e for kw in [
                "Uncaught", "TypeError", "ReferenceError", "Cannot read prop",
                "undefined is not", "ChunkLoadError",
            ])]

            if has_crash or react_errors:
                self.results.append({
                    "test": "test_export_during_load",
                    "passed": False,
                    "reason": (
                        f"JS crash during export-on-load. "
                        f"Crash overlay: {has_crash}. "
                        f"React errors: {len(react_errors)}"
                    ),
                    "js_errors": react_errors[:5],
                })
            elif not clicked:
                self.results.append({
                    "test": "test_export_during_load",
                    "passed": True,
                    "reason": "No export button found on /#/rates (may need data loaded first); no crash",
                })
            else:
                self.results.append({
                    "test": "test_export_during_load",
                    "passed": True,
                    "reason": "Export button clicked during page load with no JS crash",
                })
        finally:
            page.close()
