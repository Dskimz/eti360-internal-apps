from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PriceConfig:
    prompt_per_1m_usd: float
    completion_per_1m_usd: float


def _env_float(name: str, default: float = 0.0) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def get_price_config(provider: str) -> PriceConfig:
    provider = (provider or "").strip().lower()
    if provider == "perplexity":
        return PriceConfig(
            prompt_per_1m_usd=_env_float("PERPLEXITY_PROMPT_COST_PER_1M_USD", 0.0),
            completion_per_1m_usd=_env_float("PERPLEXITY_COMPLETION_COST_PER_1M_USD", 0.0),
        )
    if provider == "openai":
        return PriceConfig(
            prompt_per_1m_usd=_env_float("OPENAI_PROMPT_COST_PER_1M_USD", 0.0),
            completion_per_1m_usd=_env_float("OPENAI_COMPLETION_COST_PER_1M_USD", 0.0),
        )
    return PriceConfig(prompt_per_1m_usd=0.0, completion_per_1m_usd=0.0)


def estimate_cost_usd(*, provider: str, prompt_tokens: int, completion_tokens: int) -> float:
    cfg = get_price_config(provider)
    return (prompt_tokens / 1_000_000.0) * cfg.prompt_per_1m_usd + (completion_tokens / 1_000_000.0) * cfg.completion_per_1m_usd

