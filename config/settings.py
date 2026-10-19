from pathlib import Path
import os

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / ".env")


def _env_float(name, default=None):
    raw = os.getenv(name, "")
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).strip())
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid {name}={raw!r}: must be a number") from exc


def _env_int(name, default=None):
    raw = os.getenv(name, "")
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(float(str(raw).strip()))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid {name}={raw!r}: must be an integer") from exc


class Settings:
    API_KEY = os.getenv("DELTA_API_KEY", "")
    API_SECRET = os.getenv("DELTA_API_SECRET", "")

    # Delta environment selection (sandbox/production separation).
    # "" (unset) means UNKNOWN: the environment authority
    # (core/exchanges/delta/delta_environments.py) refuses
    # endpoint and credential resolution for UNKNOWN instead of
    # silently falling back. Existing client constructors keep
    # their exact production defaults; environment selection
    # happens ONLY through the explicit authority, never
    # implicitly. See docs/integrations/DELTA_ENVIRONMENTS.md.
    DELTA_ENVIRONMENT = os.getenv("DELTA_ENVIRONMENT", "")

    # Sandbox-only credentials. NEVER reinterpreted from (or as)
    # production credentials: the venue binds keys to
    # environments, and the authority refuses substitution.
    # Absent by default -> SANDBOX_CREDENTIALS = UNAVAILABLE.
    SANDBOX_API_KEY = os.getenv("DELTA_SANDBOX_API_KEY", "")
    SANDBOX_API_SECRET = os.getenv("DELTA_SANDBOX_API_SECRET", "")

    SYMBOLS = [
        symbol.strip()
        for symbol in os.getenv(
            "SYMBOLS",
            "BTCUSDT,ETHUSDT"
        ).split(",")
        if symbol.strip()
    ]

    TIMEFRAME = os.getenv("TIMEFRAME", "1m")
    PAPER_MODE = os.getenv("PAPER_MODE", "True").lower() == "true"
    LIVE_MODE = os.getenv("LIVE_MODE", "False").lower() == "true"
    LIVE_EXECUTE = os.getenv("LIVE_EXECUTE", "").lower() in ("1", "true")

    # Phase 11A: master LIVE arming flag. Default False.
    # LIVE execution requires ALL of: mode == LIVE AND
    # ALLOW_LIVE == true AND LIVE_EXECUTE == true AND
    # valid authenticated Delta credentials (see
    # core/execution/live_safety_gate.py). There is no
    # single-flag bypass: ALLOW_LIVE=true alone arms
    # nothing without the other conditions.
    ALLOW_LIVE = os.getenv("ALLOW_LIVE", "").lower() in ("1", "true")

    # Phase 11A: optional explicit execution-mode
    # override. "" (unset) derives the mode from
    # PAPER_MODE / LIVE_MODE. When set it must name
    # PAPER, LIVE, or ANALYZE -- anything else fails
    # closed with ConfigurationError (never a silent
    # fall-through to analyze-only).
    EXECUTION_MODE = os.getenv("EXECUTION_MODE", "")

    # Backtest universe selector. "all" = every product Delta
    # lists (perpetual futures, options, tokenized instruments)
    # via DeltaContractLoader().load(); "explicit" = SYMBOLS.
    # Phase 9P-d adds "provider": drive the canonical Phase 6B
    # UniverseManager with live liquidity/history providers so the
    # eligibility cascade (not SYMBOLS) decides the eligible cut.
    UNIVERSE = os.getenv("UNIVERSE", "explicit")

    # Phase 9P-d: provider-backed universe thresholds. Applied only
    # when UNIVERSE == "provider". An UNCONFIGURED (None) liquidity
    # threshold still REQUIRES a measurable volume figure (missing
    # data -> deny); a configured value adds the >= bound. Missing
    # history is always a deny when min_history_bars is configured.
    # These are OPERATOR thresholds; leaving them None means "no
    # numeric bound", NOT "no data requirement".
    MIN_QUOTE_VOLUME_24H = _env_float("MIN_QUOTE_VOLUME_24H", None)
    MIN_HISTORY_BARS = _env_int("MIN_HISTORY_BARS", None)

    # Phase 9P-d: how far back the history provider looks to count
    # available bars (in days) for each instrument's timeframe.
    PROVIDER_HISTORY_LOOKBACK_DAYS = _env_int("PROVIDER_HISTORY_LOOKBACK_DAYS", 7)

    # Phase 9P-f: Control & Monitoring API. Read-only-first, PAPER-
    # only control surface; binds to 127.0.0.1. OFF by default.
    # When enabled a CONTROL_API_TOKEN MUST be set -- an enabled
    # API with an empty token fails closed (refuses all requests).
    CONTROL_API_ENABLED = os.getenv(
        "CONTROL_API_ENABLED", "False"
    ).lower() == "true"
    CONTROL_API_TOKEN = os.getenv("CONTROL_API_TOKEN", "")
    CONTROL_API_HOST = os.getenv("CONTROL_API_HOST", "127.0.0.1")
    CONTROL_API_PORT = int(os.getenv("CONTROL_API_PORT", "8787"))
    CONTROL_API_POLL_SECONDS = float(
        os.getenv("CONTROL_API_POLL_SECONDS", "10")
    )

    # Phase 9P-k: Secure Remote / Mobile Control. The remote gateway
    # is a separate stdlib HTTPS server that sits in FRONT of the
    # localhost Control API and is gated on operator config. It is
    # OFF by default, binds to 127.0.0.1 by default (never 0.0.0.0
    # unless the operator explicitly configures that), and FAILS
    # CLOSED when enabled without the required TLS cert/key and role
    # tokens. Per-role tokens (REMOTE_VIEW_TOKEN / REMOTE_CONTROL_TOKEN
    # / REMOTE_EMERGENCY_TOKEN) and the TLS cert paths are read from
    # the environment in core/remote/gateway.py -- they are secrets /
    # operator artifacts and are not given defaults here.
    REMOTE_GATEWAY_ENABLED = os.getenv(
        "REMOTE_GATEWAY_ENABLED", "False"
    ).lower() == "true"
    REMOTE_GATEWAY_HOST = os.getenv("REMOTE_GATEWAY_HOST", "127.0.0.1")
    REMOTE_GATEWAY_PORT = int(os.getenv("REMOTE_GATEWAY_PORT", "9443"))
    AUDIT_ENABLED = os.getenv("AUDIT_ENABLED", "False").lower() == "true"
    AUDIT_PATH = os.getenv("AUDIT_PATH", str(BASE_DIR / "logs" / "remote_audit.jsonl"))


settings = Settings()