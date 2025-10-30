# main.py
import time
import json
import random
import csv
import os
import re
import math
import sys
import socket
import urllib.request
import urllib.parse
import psutil
import gspread

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, StaleElementReferenceException, NoSuchElementException


# ----------------------------
# Paths & constants
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHROME_PROFILES_DIR = PROJECT_ROOT / "chrome profiles"
SECRETS_FILE = PROJECT_ROOT / "secrets" / "auction_accounts.json"
TARGET_URL = "https://www.auction.com/"
BASE_URL = "https://www.auction.com/residential/foreclosures_at"
SPREADSHEET_ID = "1pkTmWR5rr2TFK3MNEO1mdindCk9erk4RQXItVPaEEjA" 



# ----------------------------
# Processing limits (globals)
# ----------------------------

# === Chunk knobs ===
PREV_UPDATE_CHUNK_SIZE = 500     # how many data rows per chunk (excluding header)
MAX_PREV_UPDATE_CHUNKS = 20        # how many chunks to process per run

# === Row insert knobs ===
STACK_NEW_ROWS_AT_TOP = True      # True => insert at row 2; False => append at bottom

# === Detail scrape knobs ===
NEW_HEADER = [
    "Link", "Address", "State", "Opening Bid", "Est. Market Value",
    "Auction Start Date", "Auction Start Time", "Status",
    "Completed", "Added Date"
]



# Timings
TEMPORARY_WAIT = 5
WAIT_TIME = 10
PRE_TYPE_WAIT = 1.0
MAX_LOGIN_RETRIES = 3  # fresh browser retries per profile
REFRESH_WAIT_SECONDS = 15  # 15–20 sec window as you prefer
CAPTCHA_WAIT = 3*60
WAIT_AFTER_EACH_PROFILE = 3*60
WAIT_AFTER_EACH_STATE = 15*60
WAIT_AFTER_EACH_DETAIL_PAGE = 1



# ----------------------------
# Per-profile fingerprints & headers
# ----------------------------
PROFILE_SETTINGS: Dict[str, Dict] = {
    "profile_1": {
        "profile_directory_name": "Default",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "accept_language": "en-US,en;q=0.9",
        "platform": "Windows",
        "extra_headers": {
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.auction.com/",
        },
    },
    "profile_2": {
        "profile_directory_name": "Default",
        "user_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "accept_language": "en-GB,en;q=0.8",
        "platform": "MacIntel",
        "extra_headers": {
            "Accept-Language": "en-GB,en;q=0.8",
            "Referer": "https://www.auction.com/",
        },
    },
    "profile_3": {
        "profile_directory_name": "Default",
        "user_agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "accept_language": "en-US,en;q=0.7",
        "platform": "Linux x86_64",
        "extra_headers": {
            "Accept-Language": "en-US,en;q=0.7",
            "Referer": "https://www.auction.com/",
        },
    },

    # ==============================
    # New detail-profiles begin here
    # ==============================

    "profile_4": {
        "profile_directory_name": "Default",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "accept_language": "en-US,en;q=0.9",
        "platform": "Windows",
        "extra_headers": {
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.auction.com/",
        },
    },
    "profile_5": {
        "profile_directory_name": "Default",
        "user_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "accept_language": "en-GB,en;q=0.8",
        "platform": "MacIntel",
        "extra_headers": {
            "Accept-Language": "en-GB,en;q=0.8",
            "Referer": "https://www.auction.com/",
        },
    },
    "profile_6": {
        "profile_directory_name": "Default",
        "user_agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "accept_language": "en-US,en;q=0.7",
        "platform": "Linux x86_64",
        "extra_headers": {
            "Accept-Language": "en-US,en;q=0.7",
            "Referer": "https://www.auction.com/",
        },
    },
}



# ----------------------------
# Load accounts dynamically from secrets
# ----------------------------
def load_accounts() -> Dict[str, Dict[str, str]]:
    if not SECRETS_FILE.exists():
        raise FileNotFoundError(f"❌ Missing secrets file at: {SECRETS_FILE}")
    with open(SECRETS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    accounts = {}
    for entry in data.get("accounts", []):
        profile = entry.get("profile")
        if profile:
            accounts[profile] = {
                "zone": entry.get("zone"),
                "email": entry.get("email"),
                "password": entry.get("password"),
                "states": entry.get("states", []),
            }
    print(f"🔐 Loaded {len(accounts)} profiles from secrets file.")
    return accounts

def load_detail_accounts() -> list[dict]:
    if not SECRETS_FILE.exists():
        raise FileNotFoundError(f"❌ Missing secrets file at: {SECRETS_FILE}")
    with open(SECRETS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    arr = data.get("detail_accounts", [])
    # Keep only profiles present in PROFILE_SETTINGS
    valid = [a for a in arr if a.get("profile") in PROFILE_SETTINGS]
    if not valid:
        print("⚠️ No valid detail_accounts found in secrets.")
    return valid

ACCOUNTS = load_accounts()
DETAIL_ACCOUNTS = load_detail_accounts()

def load_single_detail_driver(profile_name: str) -> tuple[str, webdriver.Chrome] | None:
    """
    Boots ONE detail driver by profile name.
    - closes stale chrome using this profile
    - opens driver
    - logs in using creds from secrets/detail_accounts
    Returns: (profile_name, driver) or None
    """

    accounts = load_detail_accounts()
    acct = next((a for a in accounts if a.get("profile") == profile_name), None)
    if not acct:
        print(f"⚠️ No credentials found for detail profile '{profile_name}'")
        return None

    email = acct.get("email", "")
    pwd   = acct.get("password", "")

    # ensure stale instance is not running
    try:
        close_profile_if_running(CHROME_PROFILES_DIR, profile_name)
    except Exception:
        pass

    drv = None
    ok = False

    for attempt in range(MAX_LOGIN_RETRIES):
        try:
            if drv:
                try: drv.quit()
                except: pass
                drv = None

            drv = get_driver(profile_name)
            time.sleep(TEMPORARY_WAIT)
            drv.get(TARGET_URL)
            w = WebDriverWait(drv, WAIT_TIME)
            time.sleep(3)

            if _detect_captcha(drv):
                print(f"⚠️ CAPTCHA detected on login page for {profile_name}. Waiting {CAPTCHA_WAIT}s...")
                time.sleep(CAPTCHA_WAIT)

            if is_logged_in(drv):
                ok = True
                break

            if email and pwd:
                sign_in(drv, email, pwd)
                time.sleep(2)
                if is_logged_in(drv):
                    ok = True
                    break
        except Exception:
            continue

    if ok and drv:
        print(f"✅ Loaded detail driver for {profile_name}")
        return (profile_name, drv)

    print(f"❌ Failed loading detail driver for {profile_name}")
    if drv:
        try: drv.quit()
        except: pass
    return None


def load_all_detail_drivers() -> list[tuple[str, webdriver.Chrome]]:
    """
    Loads ALL detail drivers defined in secrets → detail_accounts array.
    Calls load_single_detail_driver(profile) for each.
    Returns list of (profile_name, driver)
    """
    accounts = DETAIL_ACCOUNTS
    pool: list[tuple[str, webdriver.Chrome]] = []

    if not accounts:
        print("⚠️ No detail accounts found.")
        return pool

    for acct in accounts:
        pname = acct.get("profile")
        one = load_single_detail_driver(pname)
        if one:
            pool.append(one)

    if pool:
        print(f"✅ Bootstrapped {len(pool)} detail driver(s)")
    else:
        print("❌ No detail drivers successfully loaded")

    return pool


def shutdown_detail_drivers(detail_drivers: list[tuple[str, webdriver.Chrome]]) -> None:
    """Closes all loaded detail drivers"""
    for _, d in detail_drivers or []:
        try:
            d.quit()
        except Exception:
            pass


# ----------------------------
# Internal helpers
# ----------------------------

def internet_ok(
    targets=None, timeout=5, retries=3, backoff_base=2, verbose=True
) -> bool:
    """
    Returns True if we appear to have working internet access.
    Strategy per target:
      1) DNS resolve host
      2) TCP connect to host:443
      3) HEAD/GET a tiny URL (e.g., generate_204) and expect 2xx/3xx
    Retries across all targets with exponential backoff.
    """
    if targets is None:
        targets = [
            # (host_for_dns/tcp, tiny_url_for_http)
            ("www.auction.com", "https://www.auction.com/"),
            ("www.google.com", "https://www.google.com/generate_204"),
            ("cp.cloudflare.com", "https://cp.cloudflare.com/generate_204"),
        ]

    last_err = None
    for attempt in range(1, retries + 1):
        for host, url in targets:
            try:
                # DNS + TCP
                addr = socket.gethostbyname(host)
                with socket.create_connection((addr, 443), timeout=timeout):
                    pass

                # HTTP probe (HEAD preferred; some servers may block HEAD → fallback to GET)
                req = urllib.request.Request(url, method="HEAD")
                try:
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        if 200 <= resp.status < 400:
                            if verbose:
                                print(f"🌐 Internet OK via {host} ({resp.status}).")
                            return True
                except Exception:
                    # try GET once if HEAD not allowed
                    req = urllib.request.Request(url, method="GET")
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        if 200 <= resp.status < 400:
                            if verbose:
                                print(f"🌐 Internet OK via {host} ({resp.status}).")
                            return True

            except Exception as e:
                last_err = f"{host} / {url} → {e!r}"
                continue

        # backoff before next attempt across all targets
        sleep_s = min(backoff_base ** attempt, 10)
        if verbose:
            print(f"⏳ Internet check retry {attempt}/{retries} in {sleep_s}s...")
        time.sleep(sleep_s)

    if verbose:
        print(f"❌ Internet check failed after {retries} retries. Last error: {last_err}")
    return False


def close_profile_if_running(
    profiles_root: str | Path,
    profile_name: str,
    profile_dir_name: str = "Default",
    verbose: bool = True,
) -> None:
    """
    Kill any Chrome/Chromium/Edge processes using:
      --user-data-dir=<profiles_root>/<profile_name>
      --profile-directory=<profile_dir_name>   (default 'Default')

    Examples:
      close_profile_if_running_simple(PROJECT_ROOT / "chrome profiles", "profile_1")
      close_profile_if_running_simple("C:/.../chrome profiles", "profile_2", "Default")
    """
    user_data_dir = str(Path(profiles_root, profile_name).resolve())
    root_flag     = f"--user-data-dir={user_data_dir}"
    prof_flag     = f"--profile-directory={profile_dir_name}"

    if verbose:
        print(f"[cleanup] Looking for Chrome with:\n"
              f"          {root_flag}\n"
              f"          {prof_flag}")

    victims = []

    # ---- Primary path: psutil (cross-platform) ----
    if psutil is not None:
        try:
            for p in psutil.process_iter(["name", "cmdline", "pid"]):
                try:
                    name = (p.info.get("name") or "").lower()
                    if not any(k in name for k in ("chrome", "chromium", "msedge")):
                        continue
                    cmd = p.info.get("cmdline") or []
                    cmd_str = " ".join(cmd)

                    # Helpful debug
                    if verbose and ("--user-data-dir" in cmd_str):
                        if user_data_dir in cmd_str or profile_dir_name in cmd_str:
                            print(f"[cleanup] candidate PID {p.pid}: {cmd_str[:200]}...")

                    if root_flag in cmd_str and prof_flag in cmd_str:
                        victims.append(p)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        except Exception as e:
            if verbose:
                print(f"[cleanup] psutil scan error: {e}")

        if not victims:
            if verbose:
                print("[cleanup] No matching Chrome processes found for that profile.")
        else:
            if verbose:
                print(f"[cleanup] Found {len(victims)} match(es). Terminating...")

            # Terminate whole trees
            for p in victims:
                try:
                    proc = psutil.Process(p.pid)
                    for c in proc.children(recursive=True):
                        c.terminate()
                    proc.terminate()
                except Exception:
                    pass

            gone, alive = psutil.wait_procs(victims, timeout=5)
            if alive and verbose:
                print(f"[cleanup] {len(alive)} still alive; killing...")
            for p in alive:
                try:
                    proc = psutil.Process(p.pid)
                    for c in proc.children(recursive=True):
                        c.kill()
                    proc.kill()
                except Exception:
                    pass

    # ---- Windows fallback if psutil missing ----
    if psutil is None and os.name == "nt":
        try:
            import subprocess
            result = subprocess.run(
                ["wmic", "process", "where", "name='chrome.exe'",
                 "get", "ProcessId,CommandLine", "/FORMAT:CSV"],
                capture_output=True, text=True, timeout=8
            )
            out = result.stdout or ""
            pids = []
            for line in out.splitlines():
                if root_flag in line and prof_flag in line:
                    parts = line.strip().split(",")
                    pid_str = parts[-1].strip() if parts else ""
                    if pid_str.isdigit():
                        pids.append(pid_str)
            if pids and verbose:
                print(f"[cleanup] WMIC fallback killing PIDs: {pids}")
            for pid in pids:
                subprocess.run(["taskkill", "/PID", pid, "/F"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            if verbose:
                print(f"[cleanup] Windows fallback failed: {e}")

    # Small pause to release OS file locks
    time.sleep(0.8)

    # Remove Chrome lock files
    try:
        ud_path = Path(user_data_dir) / profile_dir_name
        for n in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            lp = ud_path / n
            if lp.exists():
                try:
                    lp.unlink(missing_ok=True)
                    if verbose:
                        print(f"[cleanup] Removed lock: {lp.name}")
                except Exception:
                    pass
    except Exception:
        pass

    if verbose:
        print("[cleanup] Completed.")


def _ensure_profile_dir(profile_name: str) -> Path:
    CHROME_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    p = CHROME_PROFILES_DIR / profile_name
    p.mkdir(parents=True, exist_ok=True)
    return p


def _build_options(user_data_dir: Path, profile_dir_name: str, user_agent: str,
                   accept_language: str) -> Options:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")  # full screen always
    chrome_options.add_argument(f"--user-data-dir={str(user_data_dir)}")
    chrome_options.add_argument(f"--profile-directory={profile_dir_name}")
    chrome_options.add_argument(f"--user-agent={user_agent}")
    chrome_options.add_argument(f"--lang={accept_language}")

    # Anti-detection + stability
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")

    # Disable popups & password manager
    prefs = {
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.images": 2, 

    }
    chrome_options.add_experimental_option("prefs", prefs)
    return chrome_options


def _apply_stealth_and_headers(driver: webdriver.Chrome, user_agent: str, platform: str,
                               accept_language: str, extra_headers: Dict):
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
            if (originalQuery) {
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(parameters)
                );
            }
            window.chrome = window.chrome || { runtime: {} };
        """}
    )
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {
        "userAgent": user_agent,
        "platform": platform,
        "acceptLanguage": accept_language
    })
    driver.execute_cdp_cmd("Network.enable", {})
    if extra_headers:
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": extra_headers})


def get_driver(profile_name: str) -> webdriver.Chrome:
    if profile_name not in PROFILE_SETTINGS:
        raise ValueError(f"Unknown profile '{profile_name}'. Expected: {list(PROFILE_SETTINGS.keys())}")

    cfg = PROFILE_SETTINGS[profile_name]
    user_dir = _ensure_profile_dir(profile_name)
    options = _build_options(
        user_data_dir=user_dir,
        profile_dir_name=cfg["profile_directory_name"],
        user_agent=cfg["user_agent"],
        accept_language=cfg["accept_language"],
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    _apply_stealth_and_headers(
        driver=driver,
        user_agent=cfg["user_agent"],
        platform=cfg["platform"],
        accept_language=cfg["accept_language"],
        extra_headers=cfg["extra_headers"],
    )
    return driver


# ----------------------------
# AUTH functions
# ----------------------------
def is_logged_in(driver: webdriver.Chrome) -> bool:
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-elm-id='h_user_menu']"))
        )
    except TimeoutException:
        return False

    for el in driver.find_elements(By.CSS_SELECTOR, "div[data-elm-id='h_user_menu']"):
        try:
            if "Welcome" in (el.text or ""):
                return True
        except Exception:
            continue
    return False


def sign_in(driver: webdriver.Chrome, email: str, password: str) -> None:
    wait = WebDriverWait(driver, WAIT_TIME)
    try:
        maybe_later = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[@data-elm-id='onboarding_drawer_skip_button']"))
        )
        maybe_later.click()
        print("🟢 Clicked 'Maybe later'.")
    except TimeoutException:
        print("ℹ️ 'Maybe later' not shown; continuing.")

    try:
        login_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-elm-id='h_user_login'] button"))
        )
        login_btn.click()
        print("🟢 Clicked 'Log In'.")
    except TimeoutException:
        print("ℹ️ 'Log In' button not found/clickable; continuing.")

    time.sleep(2)

    # Scope to the login modal root
    login_root = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-elm-id='auth_toggle_login']"))
    )

    # Email input
    email_input = wait.until(
        EC.visibility_of_element_located((
            By.CSS_SELECTOR,
            "[data-elm-id='auth_toggle_login_email_address_input'] input[name='email'][type='email']"
        ))
    )
    email_input.click()
    email_input.clear()
    email_input.send_keys(email)
    
    # Important: move focus so React finalizes the input
    email_input.send_keys(Keys.TAB)
    
    # Now wait for React to mount password input
    time.sleep(0.3)  # works better than long 1.5s
    
    # Locate password AFTER tab blur triggers React rerender
    pw_input = wait.until(
        EC.visibility_of_element_located((
            By.CSS_SELECTOR,
            "[data-elm-id='auth_toggle_login_password_input'] input[name='password'][type='password']"
        ))
    )
    pw_input.click()
    pw_input.clear()
    pw_input.send_keys(password)
    

    submit_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[@data-elm-id='auth_toggle_login_login_button']"))
    )
    submit_btn.click()
    print("🟢 Submitted login form.")


def refresh_page(driver, wait):

    time.sleep(REFRESH_WAIT_SECONDS)
    # --- Step 5: If "No results" icon shows, hard-refresh (cache-bust) with retries ---
    try:
        print("🔄 Checking for 'No results' and refreshing if needed...")
        # how many times to try (fallback to 3 if not provided globally)
        max_retries = int(MAX_LOGIN_RETRIES)

        def no_results_visible(short_wait=2):
            """Return True if the 'no results' icon is visible within a short wait."""
            try:
                WebDriverWait(driver, short_wait).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "i[data-elm-id='no_results_page_icon']"))
                )
                return True
            except TimeoutException:
                return False

        attempts = 0
        if no_results_visible(short_wait=2):
            print("⚠️ 'No results' detected. Starting cache-bust refresh attempts...")

        while attempts < max_retries and no_results_visible(short_wait=2):
            attempts += 1
            print(f"🔄 Refresh attempt {attempts}/{max_retries} (cache-bust) ...")
            try:
                # cache-busting hard refresh by appending _ts
                current = driver.current_url
                parsed = urllib.parse.urlparse(current)
                q = urllib.parse.parse_qs(parsed.query)
                q["_ts"] = [str(int(time.time()))]
                new_query = urllib.parse.urlencode(q, doseq=True)
                refreshed = urllib.parse.urlunparse(parsed._replace(query=new_query))
                driver.get(refreshed)
                print("🟢 Hard refresh with cache-buster.")
            except Exception:
                driver.refresh()
                print("ℹ️ Fallback: regular refresh.")

        print(f"⏳ Waiting {REFRESH_WAIT_SECONDS}s for results to render...")
        time.sleep(REFRESH_WAIT_SECONDS)

        if no_results_visible(short_wait=2):
            print("❌ Still seeing 'No results' after refresh attempts.")
        else:
            if attempts > 0:
                print("✅ Results appeared after refresh.")
            else:
                print("ℹ️ 'No results' icon not present; no refresh needed.")

    except Exception as e:
        print(f"⚠️ Refresh logic error: {e}")

    # (NEW) 0) If tooltip/nudge close icon present, click it first
    try:
        tooltip_close = driver.find_element(By.CSS_SELECTOR, "i[data-elm-id='header_nudge_preferences_tooltip_close_icon']")
        driver.execute_script("arguments[0].click();", tooltip_close)
        time.sleep(0.2)  # small delay to let UI collapse
    except Exception:
        pass

   # --- Step 6: Open 'Layout' and click "List" option; ensure menu closes ---

    try:
        print("🔄 Ensuring 'List' layout is selected...")
        # 1) Open the Layout menu
        layout_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[.//span[normalize-space()='Layout']]"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", layout_btn)
        layout_btn.click()   # native click to open
        print("🟢 Opened 'Layout' menu.")
        time.sleep(1)

        # 2) Find the "List" option (role=option + text)
        list_opt = wait.until(
            EC.visibility_of_element_located((
                By.XPATH, "//div[@role='option' and .//span[normalize-space()='List']]"
            ))
        )

        # If already selected, menu should close on outside click; we still try to close consistently
        already_selected = (
            (list_opt.get_attribute("aria-checked") or "").lower() == "true" or
            (list_opt.get_attribute("aria-selected") or "").lower() == "true"
        )

        # 3) Prefer a native user click (trusted)
        try:
            ActionChains(driver).move_to_element(list_opt).pause(0.05).click().perform()
        except Exception:
            # fallback to JS click if needed
            driver.execute_script("arguments[0].click();", list_opt)

        # 4) Wait for the menu to close (the option becomes stale/hidden)
        closed = False
        t0 = time.time()
        while time.time() - t0 < 3.0:
            try:
                # stale or hidden means closed
                if not list_opt.is_displayed():
                    closed = True
                    break
            except StaleElementReferenceException:
                closed = True
                break
            time.sleep(0.1)

        # 5) If still open, try Escape; if still open, toggle the Layout button again
        if not closed:
            try:
                # send Escape to the page to close popups/menus
                ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(0.15)
                # quick re-check
                # try to re-find the option; if not visible, menu closed
                tmp = driver.find_elements(By.XPATH, "//div[@role='option' and .//span[normalize-space()='List']]")
                if not tmp or not tmp[0].is_displayed():
                    closed = True
            except Exception:
                pass

        if not closed:
            try:
                # toggle the button to close
                layout_btn = driver.find_element(By.XPATH, "//button[.//span[normalize-space()='Layout']]")
                layout_btn.click()
                closed = True
            except Exception:
                pass

        if closed:
            print("🟢 Switched to 'List' and dropdown closed.")
        else:
            print("⚠️ 'List' clicked but dropdown appears to remain open.")

    except TimeoutException:
        print("❌ 'Layout' button or 'List' option not found.")


    time.sleep(1.5)  # slight pause to stabilize DOM


def _detect_captcha(driver: webdriver.Chrome) -> bool:
    try:
        time.sleep(5)  # small wait to let any captcha load
        # 1. Title heuristics
        title_text = (driver.title or "").lower()
        if "captcha" in title_text:
            return True
        if "i am human" in title_text or "i'm not a robot" in title_text:
            return True

        # 2. DOM markers / known captcha elements
        if driver.find_elements(
            By.CSS_SELECTOR,
            "iframe[src*='captcha'], .g-recaptcha, #challenge-running, #challenge-stage"
        ):
            return True

        # 3. Page text heuristics (case-insensitive substring search)
        page_txt = (driver.page_source or "").lower()

        captcha_indicators = [
            "why am i seeing this page",
            "i'm not a robot",
            "i am human",
            "additional security check is required",
            "additional security check required",
            # brand-specific text you mentioned:
            "www.giffgaaf.com - additional security check is required",
        ]

        for phrase in captcha_indicators:
            if phrase in page_txt:
                return True

    except Exception:
        # any unexpected issue -> fail closed (assume no captcha)
        pass

    return False


# --- Driver cooldown / rehydration helpers ---

def cooldown_add(cooldown_list: list[dict], profile_name: str, entries_until_retry: int = 20):
    """Remember to try re-opening this profile after N entries have been processed."""
    cooldown_list.append({"profile": profile_name, "remaining": entries_until_retry})

def cooldown_tick_and_try_rehydrate(cooldown_list: list[dict], detail_pool: list[tuple[str, webdriver.Chrome]]):
    """
    Decrement the 'remaining' counter for each cooldown item. When it reaches 0,
    attempt to re-open that profile driver. If successful, put it back into detail_pool.
    """
    # iterate over a shallow copy so we can remove while iterating
    for item in cooldown_list[:]:
        item["remaining"] -= 1
        if item["remaining"] <= 0:
            # try to bring it back
            res = load_single_detail_driver(item["profile"])
            if res:
                detail_pool.append(res)
                cooldown_list.remove(item)
            else:
                # if re-open failed, give it another full cooldown window
                item["remaining"] = 20



# ---------- Google Sheets helpers (service account in ./secrets/sheet_credentials.json) ----------
def _open_zone_worksheet(zone_name: str):

    cred_path = Path.cwd() / "secrets" / "sheet_credentials.json"
    if not cred_path.exists():
        alt = Path.cwd().parent / "secrets" / "sheet_credentials.json"
        if alt.exists():
            cred_path = alt
    if not cred_path.exists():
        raise FileNotFoundError(f"Sheet credentials not found at {cred_path}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(str(cred_path), scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(zone_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=zone_name, rows=20000, cols=30)

    # Ensure required header (order matters)
    required = NEW_HEADER
    values = ws.get_values("A1:J1")
    header = values[0] if values else []
    if header != required:
        ws.update("A1:J1", [required])
    return ws



# =========================
# ENRICHMENT HOOKS (you fill later)
# =========================

def scrape_row_with_driver(
    driver,
    link: str,
    state: str,
    header_cols: list[str] = NEW_HEADER,
    current_row: dict | None = None,
    return_dict: bool = False,  # False => list for inserts; True => dict for updates
):
    try:
        driver.get(link)
        time.sleep(WAIT_AFTER_EACH_DETAIL_PAGE)
        WebDriverWait(driver, WAIT_TIME).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-elm-id='auction-detail-box-status']")))

        if _detect_captcha(driver):
            return {"__captcha__": True}

        def t(css: str) -> str:
            try:
                return (driver.find_element(By.CSS_SELECTOR, css).text or "").strip()
            except Exception:
                return ""

        def first_line(s: str) -> str:
            if not s:
                return ""
            s = s.replace("\r", "")
            s = s.split("\n", 1)[0]
            s = s.split("Add to calendar", 1)[0]
            return s.strip()

        def strip_weekday_prefix(date_text: str) -> str:
            """
            Remove weekday prefixes like:
              'Monday, Oct 27, 2025' -> 'Oct 27, 2025'
              'Mon, Oct 27, 2025'    -> 'Oct 27, 2025'
            If no weekday present, returns input unchanged.
            """
            if not date_text:
                return ""
            s = date_text.strip()
            # if there's a comma and the token before it is alphabetic, drop it
            if "," in s:
                first, rest = s.split(",", 1)
                # accept both full and short weekday names (alphabetic token)
                if first.strip().isalpha():
                    return rest.strip()
            return s

        def parse_date_time_from_text(s: str) -> tuple[str, str]:
            """
            Given a string like 'Oct 27, 2025 7:00 AM' or 'October 27, 2025 10:30 PM',
            return ('Oct 27, 2025', '7:00 AM').
            If time is missing, returns ('Oct 27, 2025', '').
            """
            if not s:
                return "", ""
            # normalize spaces
            s = " ".join(s.split())
            # Regex: Month Day, Year [Time AM/PM]
            # Month can be short/long; AM/PM can be lower or upper.
            m = re.match(
                r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})(?:\s+(?P<time>\d{1,2}:\d{2}\s*[APap][Mm]))?$",
                s
            )
            if not m:
                # If it doesn't match, try removing any weekday prefix first then retry once
                s2 = strip_weekday_prefix(s)
                m = re.match(
                    r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})(?:\s+(?P<time>\d{1,2}:\d{2}\s*[APap][Mm]))?$",
                    s2
                )
                if not m:
                    return "", ""

            date_part = f"{m.group('month')} {m.group('day')}, {m.group('year')}"
            time_part = (m.group('time') or "").upper().replace("  ", " ").strip()
            return date_part, time_part

        # --- scrape raw values ---
        opening_bid  = t("[data-elm-id='opening_bid_value']")
        addr_raw = t("[data-elm-id='property_header_address']")
        # convert newline → ", "
        addr_line = " ".join(addr_raw.split())   # normalize whitespace
        addr_line = addr_line.replace(" ,", ",") # cleanup if needed
        full_addr = addr_line


        est_mv       = t("[data-elm-id='arv_value']")
        auction_date_raw = first_line(t("[data-elm-id='date_value']"))
        auction_time = t("[data-elm-id='auction_start_time_value']")
        status_text  = t("[data-elm-id='property_gallery_status_label']")

        # 1) Strip weekday from auction_date_raw before finalizing
        auction_date = strip_weekday_prefix(auction_date_raw)

        # 2) Fallback: if both date & time are blank, parse the auction duration range
        if not auction_date and not auction_time:
            range_text = t("[data-elm-id='auction_duration_date_range']")
            if range_text:
                # take left side before dash: 'Oct 27, 2025 7:00 AM - Oct 29, 2025' -> 'Oct 27, 2025 7:00 AM'
                left_side = range_text.split(" - ", 1)[0].strip()
                d_part, t_part = parse_date_time_from_text(left_side)
                auction_date = d_part or auction_date
                auction_time = t_part or auction_time

        prev = dict(current_row or {})

        def keep_prev_if_blank(new_val: str, prev_key: str) -> str:
            nv = (new_val or "").strip()
            return nv if nv else (prev.get(prev_key) or "").strip()

        merged = {
            "Link": link,
            "Address": keep_prev_if_blank(full_addr, "Address"),
            "State": (state or prev.get("State") or "").strip(),
            "Opening Bid": keep_prev_if_blank(opening_bid, "Opening Bid"),
            "Est. Market Value": keep_prev_if_blank(est_mv, "Est. Market Value"),
            "Auction Start Date": keep_prev_if_blank(auction_date, "Auction Start Date"),
            "Auction Start Time": keep_prev_if_blank(auction_time, "Auction Start Time"),
            "Status": keep_prev_if_blank(status_text, "Status"),
        }

        # Completed
        ob_norm = (merged["Opening Bid"] or "").strip().lower()
        emv_has_value = bool((merged["Est. Market Value"] or "").strip())  # "Not available" counts
        ob_present_and_real = bool(ob_norm) and ob_norm not in {"tbd", ""}
        merged["Completed"] = "1" if (emv_has_value and ob_present_and_real) else "0"

        # 🔒 Hard-close rule: if Auction Start Date == (today - 1 day) → Completed = 1 regardless
        auct_str = (merged.get("Auction Start Date") or "").strip()
        if auct_str:
            try:
                auct_d = datetime.strptime(auct_str, "%b %d, %Y").date()
                if auct_d == (date.today() - timedelta(days=1)):
                    merged["Completed"] = "1"
            except Exception:
                pass  # if unparsable, ignore and keep prior Completed logic

        merged["Added Date"] = datetime.now().strftime("%b %d, %Y")

        if return_dict:
            print("🟢 Scraped data (dict):", merged)
            return merged
        

        return [merged.get(col, "") for col in header_cols]

    except Exception:
        return {} if return_dict else []


def insert_new_links_first(
    zone: str,
    state: str,
    new_links: list[str],
    detail_drivers: list[tuple[str, webdriver.Chrome]],
) -> dict:
    stats = {"attempted": 0, "inserted": 0, "failed": 0}
    if not new_links:
        return stats

    ws = _open_zone_worksheet(zone)
    header_cols = NEW_HEADER

    def pick_driver(i: int) -> tuple[str, webdriver.Chrome]:
        return detail_drivers[i % len(detail_drivers)]

    r_i = 0
    cooldown: list[dict] = []  # <-- NEW

    for raw_link in new_links:
        link = (raw_link or "").strip()
        if not link:
            continue

        stats["attempted"] += 1
        if not detail_drivers:
            stats["failed"] += 1
            # still tick cooldown so drivers may come back
            cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
            if not detail_drivers:
                # no available drivers right now; move to next link
                continue

        pname, drv = pick_driver(r_i); r_i += 1

        vals = scrape_row_with_driver(drv, link, state, header_cols)

        # --- CAPTCHA handling: drop this driver, add to cooldown, retry once with next
        if isinstance(vals, dict) and vals.get("__captcha__"):
            try:
                drv.quit()
            except Exception:
                pass
            # remove from pool
            detail_drivers[:] = [(n, d) for (n, d) in detail_drivers if d is not drv]
            # add to cooldown (retry after ~20 entries)
            cooldown_add(cooldown, pname, entries_until_retry=20)
            r_i -= 1

            # try next available driver
            if not detail_drivers:
                stats["failed"] += 1
                cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
                continue
            pname, drv = pick_driver(r_i); r_i += 1
            vals = scrape_row_with_driver(drv, link, state, header_cols)

        if not (isinstance(vals, list) and vals):
            stats["failed"] += 1
            # tick cooldown after each link attempt
            cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
            continue

        try:
            if STACK_NEW_ROWS_AT_TOP and hasattr(ws, "insert_row"):
                ws.insert_row(vals, index=2, value_input_option="USER_ENTERED")
            else:
                ws.append_row(vals, value_input_option="USER_ENTERED")
            stats["inserted"] += 1
        except Exception:
            stats["failed"] += 1

        # --- after each processed link, tick cooldowns and try to rehydrate drivers
        cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)

    return stats


def _parse_auction_date_to_date(s: str) -> date | None:
    """Accepts 'Oct 27, 2025' and also 'Monday, Oct 27, 2025' (weekday stripped)."""
    if not s:
        return None
    txt = s.strip()
    if "," in txt:
        first = txt.split(",", 1)[0].strip()
        if first.isalpha() and len(first) > 3:
            txt = txt.split(",", 1)[1].strip()
    try:
        return datetime.strptime(txt, "%b %d, %Y").date()
    except Exception:
        return None

def _row_dict_from_values(header: list[str], row_vals: list[str]) -> dict:
    row = {}
    for i, name in enumerate(header):
        row[name] = (row_vals[i] if i < len(row_vals) else "").strip()
    return row

def _is_candidate_row_full(row: dict, today: date) -> bool:
    """
    Filter rules:
      - Completed == "0"
      - Added Date != today (date-only compare)
      - Auction Start Date blank OR within ±1 day of today
    """
    # Completed must be "0"
    if (row.get("Completed") or "").strip() != "0":
        return False

    # Skip rows added today (regardless of time)
    added_raw = (row.get("Added Date") or "").strip()
    if added_raw:
        # expected format we stamp: "YYYY-MM-DD HH:MM:SS" -> compare date-only
        try:
            added_d = datetime.strptime(added_raw, "%b %d, %Y").date()
            if added_d == today:
                return False
        except Exception:
            # If Added Date is malformed, treat as not-today and continue
            pass

    # Auction date condition
    auct_raw = (row.get("Auction Start Date") or "").strip()
    if not auct_raw:
        return True  # blank qualifies
    auct_d = _parse_auction_date_to_date(auct_raw)
    if auct_d is None:
        # treat unparseable as blank per your rule
        return True
    return abs((auct_d - today).days) <= 1


# ---- main chunked updater (full-read per chunk, in-place updates) ----
def update_prev_entries_for_zone_chunked_fullread(
    zone: str,
    detail_drivers: list[tuple[str, webdriver.Chrome]],
    chunk_size: int = PREV_UPDATE_CHUNK_SIZE,
    max_chunks: int | None = MAX_PREV_UPDATE_CHUNKS,
) -> dict:
    stats = {
        "chunks_scanned": 0,
        "candidates": 0,
        "attempted": 0,
        "updated": 0,
        "failed": 0,
        "captchas": 0,
    }
    if not detail_drivers:
        print("⚠️ No detail drivers available for prev update.")
        return stats

    ws = _open_zone_worksheet(zone)
    today = date.today()

    header_vals = ws.get_values("A1:J1")
    header = header_vals[0] if header_vals else NEW_HEADER

    try:
        colA = ws.col_values(1)
        last_row = len(colA)
    except Exception:
        last_row = 1

    if last_row <= 1:
        return stats

    data_start = 2
    data_end = last_row

    def pick_driver(i: int) -> tuple[str, webdriver.Chrome]:
        return detail_drivers[i % len(detail_drivers)]

    r_i = 0
    chunk_idx = 0
    start = data_start

    cooldown: list[dict] = []  # <-- NEW

    while start <= data_end:
        if max_chunks is not None and chunk_idx >= max_chunks:
            break
        end = min(start + chunk_size - 1, data_end)

        try:
            rows = ws.get_values(f"A{start}:J{end}")
        except Exception:
            rows = []

        rownums = []
        rowdicts = []
        for offset, row_vals in enumerate(rows):
            rownum = start + offset
            row_dict = _row_dict_from_values(header, row_vals)
            if _is_candidate_row_full(row_dict, today):
                rownums.append(rownum)
                rowdicts.append(row_dict)

        stats["chunks_scanned"] += 1
        stats["candidates"] += len(rownums)
        chunk_idx += 1

        for rownum, row_dict in zip(rownums, rowdicts):
            # If pool is empty, try to rehydrate first
            if not detail_drivers:
                cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
                if not detail_drivers:
                    stats["failed"] += 1
                    continue

            link = (row_dict.get("Link") or "").strip()
            state = (row_dict.get("State") or "").strip()
            if not link:
                stats["failed"] += 1
                cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
                continue

            stats["attempted"] += 1

            pname, drv = pick_driver(r_i); r_i += 1

            updated = scrape_row_with_driver(
                drv, link, state,
                header_cols=NEW_HEADER,
                current_row=row_dict,
                return_dict=True
            )

            if isinstance(updated, dict) and updated.get("__captcha__"):
                stats["captchas"] += 1
                # retire the driver to cooldown
                try: drv.quit()
                except Exception: pass
                detail_drivers[:] = [(n, d) for (n, d) in detail_drivers if d is not drv]
                cooldown_add(cooldown, pname, entries_until_retry=20)
                r_i -= 1

                # retry once with next available driver
                if not detail_drivers:
                    cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
                    if not detail_drivers:
                        stats["failed"] += 1
                        continue
                pname, drv = pick_driver(r_i); r_i += 1
                updated = scrape_row_with_driver(
                    drv, link, state,
                    header_cols=NEW_HEADER,
                    current_row=row_dict,
                    return_dict=True
                )

            if not isinstance(updated, dict) or not updated:
                stats["failed"] += 1
                cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)
                continue

            try:
                ws.update(f"A{rownum}:J{rownum}", [[updated.get(c, "") for c in NEW_HEADER]])
                stats["updated"] += 1
            except Exception:
                stats["failed"] += 1

            # after each processed row, tick cooldowns and try to rehydrate drivers
            cooldown_tick_and_try_rehydrate(cooldown, detail_drivers)

        start = end + 1

    return stats




def get_links_and_status(driver, wait, states, profile_name=None, zone=None):
    """
    Collects & saves new favorites per state (no return).
    - New links are gathered in strict position order.
    - Previous rows are fetched from Google Sheets (AUCTION.COM / <Zone>) where:
        State == current_state AND Opening Bid is blank/TBD (these rows are removed from the sheet)
    - Merges new links (Link+State only) with previous rows.
    - Processes each merged row immediately via enrich_and_write_row(driver, ws, header_cols, row).
    - Prints: "new links: X, previous rows: Y, processed: Z, fail: W" per state.

    UPDATED SCANNING (only this part changed):
    - PHASE 1: Downward scan pos=0..total-1:
        • If pos not in DOM -> scroll down +350px while scrollTop < (maxScroll-700)
        • After ceiling reached, continue pos++ without more downward scroll
        • If 3 consecutive true 'misses' (pos not in DOM) AFTER ceiling -> break to Phase 2
        • Placeholders DO NOT count as misses; real cards reset the miss counter
    - PHASE 2: Upward scan pos=total-1..0:
        • If pos not in DOM -> scroll up -400px while scrollTop > startScrollTop
        • Stop Phase 2 when scrollTop <= startScrollTop
        • Separate counters for downward vs upward passes
    - Placeholders (data-elm-id="asset_inject_placeholder_root") are skipped and NOT counted as already-saved.
    """

    def abs_url(href: str) -> str:
        return urllib.parse.urljoin(TARGET_URL, href)

    # -------- search bar --------
    def clear_search_input():
        container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ui.icon.input")))
        try:
            del_icon = container.find_element(By.CSS_SELECTOR, "i[data-elm-id='desktop_search_input_delete_icon']")
            driver.execute_script("arguments[0].click();", del_icon)
            time.sleep(0.05)
        except Exception:
            pass
        inp = wait.until(EC.visibility_of_element_located((
            By.CSS_SELECTOR, "input[aria-label='desktop_search_input_search'][name='Search']"
        )))
        inp.click()
        inp.send_keys(Keys.CONTROL, "a")
        inp.send_keys(Keys.DELETE)
        time.sleep(0.05)
        return inp

    def click_search_icon():
        icon = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "i[data-elm-id='desktop_search_input_search_icon']")))
        driver.execute_script("arguments[0].click();", icon)

    # -------- list structure --------
    def get_asset_list_container():
        root = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-elm-id='asset-list']")))
        lst  = root.find_element(By.CSS_SELECTOR, "div[class*='__list--']")  # virtualized, overflow:auto
        return root, lst

    def get_total_properties_if_any(root_block):
        try:
            cnt_el = root_block.find_element(By.CSS_SELECTOR, "span[data-elm-id='asset-list_totals_in_count']")
            txt = (cnt_el.text or "").strip().replace(",", "")
            n = int(re.findall(r"\d+", txt)[0])
            if n > 500:
                n=500
            print(f"📊 Total properties reported: {n}")
            return n
        except Exception:
            return None

    # -------- favorites (unchanged) --------
    def is_card_saved(card_root) -> bool:
        try:
            heart = card_root.find_element(By.CSS_SELECTOR, "i[data-elm-id^='save_property_icon_']")
            return "is-saved" in ((heart.get_attribute("class") or ""))
        except Exception:
            return False

    def save_card(card_root) -> bool:
        def _click_heart():
            heart_el = card_root.find_element(By.CSS_SELECTOR, "i[data-elm-id^='save_property_icon_']")
            driver.execute_script("arguments[0].click();", heart_el)
            return heart_el

        # first try
        try:
            heart = _click_heart()
            time.sleep(2)
            return True
        except Exception:
            pass

        return False

    # -------- card resolver (PURE DOM lookup; NO scrolling here) --------
    def find_card_by_position(list_container, pos):
        """
        Return:
          - the REAL card root (data-elm-id='asset_*_root') if present in DOM
          - OR the placeholder root (data-elm-id='asset_inject_placeholder_root') if that's what's at this position
          - OR None if nothing for this position is currently rendered.

        We do not scroll in here — scrolling is handled by the main loops.
        """
        try:
            # 1) find ANY node carrying this data-position (wrapper/child/placeholder/real)
            node = driver.execute_script("""
                const cont = arguments[0];
                const pos  = arguments[1].toString();
                return cont.querySelector(`[data-position="${pos}"]`);
            """, list_container, pos)
            if not node:
                return None

            # 2) normalize: climb to closest real card root if present; else keep node (placeholder)
            card = driver.execute_script("""
                const n = arguments[0];
                return n.closest("div[data-elm-id^='asset_'][data-elm-id$='_root']") || n;
            """, node)
            return card
        except StaleElementReferenceException:
            # list container went stale; let caller reacquire it
            return None
        except Exception:
            return None


    detail_pool = load_all_detail_drivers()


    # -------- main loop --------
    if profile_name or zone:
        print(f"🟦 Profile: {profile_name or '-'}  |  Zone: {zone or '-'}")

    for state in states:
        print(f"🌎 Searching state: {state} ...")

        # search
        inp = clear_search_input()
        inp.send_keys(state)
        click_search_icon()

        refresh_page(driver, wait)

        # list containers
        try:
            root_block, list_container = get_asset_list_container()
        except TimeoutException:
            print(f"⚠️ No properties found for {state}")
            continue

        total = get_total_properties_if_any(root_block)
        if total is None:
            print("⚠️ No total reported; skipping state (per new spec we assume total exists).")
            continue

        # ====== PREP SCROLL METRICS ======
        try:
            start_scroll_top = driver.execute_script("return arguments[0].scrollTop;", list_container)
            max_scroll = driver.execute_script("return arguments[0].scrollHeight - arguments[0].clientHeight;", list_container)
            scroll_limit = max(0, (max_scroll or 0) - 700)  # stop downward scrolling at this limit
        except Exception:
            start_scroll_top, scroll_limit = 0, 0

        # ====== COUNTERS ======
        newly_saved_down = []   # links saved during DOWNWARD phase
        newly_saved_up   = []   # links saved during UPWARD phase
        down_new = down_already = down_placeholders = 0
        up_new   = up_already   = up_placeholders   = 0

        # ====== PHASE 1: DOWNWARD SCAN (pos 0 .. total-1) ======
        consec_misses_after_ceiling = 0
        hit_ceiling = False

        pos = 0
        while pos < total + 1:
            try:
                card = find_card_by_position(list_container, pos)

                if card is not None:
                    # Bring into center (stabilize viewport). Silent per your request.
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
                    except Exception:
                        pass

                    data_id = (card.get_attribute("data-elm-id") or "")

                    # Placeholder: skip; DOES NOT count as a miss; reset miss counter
                    if data_id == "asset_inject_placeholder_root":
                        down_placeholders += 1
                        consec_misses_after_ceiling = 0
                        pos += 1
                        continue

                    # Real card (asset_*_root) — process
                    if data_id.startswith("asset_") and data_id.endswith("_root"):
                        # link
                        try:
                            a = card.find_element(By.CSS_SELECTOR, "a[href^='/details/']")
                            link = abs_url(a.get_attribute("href") or "")
                        except Exception:
                            link = ""

                        # already saved?
                        try:
                            if is_card_saved(card):
                                down_already += 1
                            else:
                                if save_card(card):
                                    if link:
                                        newly_saved_down.append(link)
                                    down_new += 1
                                else:
                                    # keep behavior silent on failed toggle
                                    pass
                        except StaleElementReferenceException:
                            # If heart went stale, treat as miss reset (it's present though)
                            pass

                        consec_misses_after_ceiling = 0
                        pos += 1
                        continue

                    # Some other node (unlikely) — treat as not found now
                    # fall through to "not found" handling

                # ---- Not found in DOM at this position ----
                # If we haven't hit the downward ceiling, scroll down +350 and retry SAME pos
                current_top = 0
                try:
                    current_top = driver.execute_script("return arguments[0].scrollTop;", list_container)
                except Exception:
                    pass

                if not hit_ceiling and current_top < scroll_limit:
                    try:
                        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 350;", list_container)
                        time.sleep(2)
                    except StaleElementReferenceException:
                        # Reacquire container on staleness
                        try:
                            _, list_container = get_asset_list_container()
                        except Exception:
                            break
                    except Exception:
                        pass
                    # retry SAME pos again
                    continue
                else:
                    # We are at/over ceiling: DO NOT SCROLL DOWN ANYMORE.
                    hit_ceiling = True
                    # This counts as a TRUE MISS (placeholder != miss, but absent DOM == miss)
                    consec_misses_after_ceiling += 1
                    if consec_misses_after_ceiling >= 3:
                        # Enter Phase 2
                        break
                    # advance to next position even if missing (per your rule)
                    pos += 1
                    continue

            except StaleElementReferenceException:
                # Reacquire container then retry this pos
                try:
                    _, list_container = get_asset_list_container()
                except Exception:
                    break
                continue

        # ====== PHASE 2: UPWARD SCAN (pos total-1 .. 0) ======
        # Always start from total-1 in reverse, regardless where downward stopped.
        pos = total
        while pos >= 0:
            try:
                card = find_card_by_position(list_container, pos)

                if card is None:
                    # If not found, try scrolling UP -400px as long as we're above the starting top.
                    try:
                        current_top = driver.execute_script("return arguments[0].scrollTop;", list_container)
                    except Exception:
                        current_top = 0

                    if current_top <= start_scroll_top:
                        # We've returned to the starting top — stop UPWARD phase strictly (per A).
                        break

                    try:
                        driver.execute_script("arguments[0].scrollTop = Math.max(0, arguments[0].scrollTop - 400);", list_container)
                        time.sleep(2)
                    except StaleElementReferenceException:
                        try:
                            _, list_container = get_asset_list_container()
                        except Exception:
                            break
                    except Exception:
                        pass

                    # retry same pos after scrolling up
                    continue

                # found a node — normalize
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
                except Exception:
                    pass

                data_id = (card.get_attribute("data-elm-id") or "")

                if data_id == "asset_inject_placeholder_root":
                    up_placeholders += 1
                    pos -= 1
                    continue

                if data_id.startswith("asset_") and data_id.endswith("_root"):
                    # link
                    try:
                        a = card.find_element(By.CSS_SELECTOR, "a[href^='/details/']")
                        link = abs_url(a.get_attribute("href") or "")
                    except Exception:
                        link = ""

                    try:
                        if is_card_saved(card):
                            up_already += 1
                        else:
                            if save_card(card):

                                if link:
                                    newly_saved_up.append(link)
                                up_new += 1
                            else:
                                pass
                    except StaleElementReferenceException:
                        pass

                    pos -= 1
                    continue

                # any other node — just move on
                pos -= 1
                continue

            except StaleElementReferenceException:
                # reacquire container then retry this pos
                try:
                    _, list_container = get_asset_list_container()
                except Exception:
                    break
                continue

        # ====== MERGE NEW LINKS FROM BOTH PASSES ======
        newly_saved_this_state = newly_saved_down + newly_saved_up
        print(f"🔖 Total newly saved links for {state}: {len(newly_saved_this_state)}")


        # ====== PRINT SCAN STATS (separate downward / upward) ======
        print("⬇️ Downward scan:")
        print(f"   newly saved: {down_new}")
        print(f"   already saved: {down_already}")
        print(f"   placeholders skipped: {down_placeholders}")
        # (We didn't keep a separate total 'down_misses' count, only the consecutive breaker, per your spec.)

        print("⬆️ Upward scan:")
        print(f"   newly saved: {up_new}")
        print(f"   already saved: {up_already}")
        print(f"   placeholders skipped: {up_placeholders}")

        try:
            print(f"🔄 Fetching new listings for {state} ...")
            new_stats = insert_new_links_first(
                zone=zone,
                state=state,
                new_links=newly_saved_this_state,
                detail_drivers=detail_pool,
            )
            print(f"🧩 New rows inserted: {new_stats}")
        except Exception as e:
            print(f"⚠️ NEW Insertion pipeline error for {state}: {e}")
        time.sleep(WAIT_AFTER_EACH_STATE)

    try:    
        upd_stats = update_prev_entries_for_zone_chunked_fullread(
        zone=zone,
        detail_drivers=detail_pool,                 # your preloaded drivers
        chunk_size=PREV_UPDATE_CHUNK_SIZE,
        max_chunks=MAX_PREV_UPDATE_CHUNKS,
        )
        print(f"🔁 Prev update (chunked) stats: {upd_stats}")
    except Exception as e:
        print(f"⚠️ Prev update pipeline error for zone {zone}: {e}")
        upd_stats = {}
    finally:
        shutdown_detail_drivers(detail_pool)


    if profile_name or zone:
        print(f"✅ Completed all states for {profile_name or '-'} ({zone or '-'})")


# ----------------------------
# Main flow
# ----------------------------
def main():
    
    profiles_in_order = ["profile_1", "profile_2", "profile_3"]

    site_url = globals().get("BASE_URL")

    if not internet_ok():
        print("❌ No internet connectivity detected. Aborting before launching browsers.")
        return

    try:
        for name in profiles_in_order:
            creds = ACCOUNTS[name]
            print(f"\n🌐 Opening {name} ({creds['zone']}) ...")

            attempt = 0
            logged_in = False
            drv = None
            wait = None

            # ---------- LOGIN / OPEN ----------
            while attempt < MAX_LOGIN_RETRIES and not logged_in:
                # 🔻 ensure no stale chrome still using this profile
                try:
                    close_profile_if_running(CHROME_PROFILES_DIR, name)
                except Exception as e:
                    print(f"ℹ️ Profile cleanup warning for {name}: {e}")

                # If we already opened a browser this loop, close before retrying
                if drv:
                    try:
                        drv.quit()
                    except Exception:
                        pass
                    drv = None

                # Fresh browser instance
                drv = get_driver(name)

                # Small stagger before navigation
                time.sleep(TEMPORARY_WAIT)

                drv.get(site_url)
                wait = WebDriverWait(drv, WAIT_TIME)

                if _detect_captcha(drv):
                    print("⚠️ CAPTCHA detected. Waiting...")
                    time.sleep(CAPTCHA_WAIT)
                    attempt += 1
                    continue

                # Quick pre-check for existing login
                if is_logged_in(drv):
                    print("✅ Already logged in (found 'Welcome').")
                    logged_in = True
                    break

                # Attempt login
                print(f"🔐 Not logged in. Signing in as {creds['email']} ...")
                sign_in(drv, creds["email"], creds["password"])
                time.sleep(2)

                # Verify login
                if is_logged_in(drv):
                    print("✅ Login verified (found 'Welcome').")
                    logged_in = True
                    break

                attempt += 1
                print(f"⚠️ Login attempt {attempt} failed; retrying with a fresh browser...")

            if not logged_in:
                print(f"❌ Failed to log in after {MAX_LOGIN_RETRIES} attempts for {name}. Skipping this profile.")
                if drv:
                    try:
                        drv.quit()
                        print(f"🧹 Closed browser for {name}.")
                    except Exception:
                        pass
                continue

            # ---------- WORK FOR THIS PROFILE ----------
            try:
                states = creds["states"]
                get_links_and_status(
                    drv,
                    wait,
                    states,
                    profile_name=name,
                    zone=creds["zone"],
                )
            finally:
                # Always close this profile's browser before moving on
                try:
                    if drv:
                        drv.quit()
                        print(f"🧹 Closed browser for {name}.")
                except Exception:
                    pass
                time.sleep(WAIT_AFTER_EACH_PROFILE)

        print("\n✅ All profiles processed. No browsers left open.")

    except KeyboardInterrupt:
        print("\n⛔ Interrupted by user. Any open browser for the current profile was closed in the cleanup.")


if __name__ == "__main__":
    main()

def test_insert_links(zone: str, state: str, links: list[str]):
    """
    Manual test helper: loads *detail drivers*, calls insert_new_links_first
    WITHOUT any scanning. Used to verify insertion works correctly.
    """
    # 1) load detail drivers
    detail_pool = load_all_detail_drivers()
    if not detail_pool:
        print("❌ No detail drivers could be loaded.")
        return

    # 2) call the insert function
    try:
        stats = insert_new_links_first(
            zone=zone,
            state=state,
            new_links=links,
            detail_drivers=detail_pool
        )
        print(f"✅ TEST INSERT RESULT: {stats}")
    finally:
        shutdown_detail_drivers(detail_pool)
