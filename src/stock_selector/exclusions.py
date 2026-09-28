"""Tradability, event, restriction and circuit exclusions (spec section 14).

Event and restriction data arrive through the data-provider interface; this
module only interprets flags. ``None`` means the dataset was unavailable and
the configured fail-open / fail-closed policy applies, always with a warning.
"""
from __future__ import annotations

from .config import POLICY_FAIL_CLOSED, SelectorConfig
from .models import PreCloseSnapshot, UniverseEntry


def resolve_optional_flag(
    value: bool | None,
    policy: str,
    warnings: list[str],
    unavailable_warning: str,
) -> tuple[bool, bool]:
    """Return (effective_risk_flag, data_available).

    ``value`` is the raw risk flag (True == risky/blocked). When it is None
    the dataset is unavailable: fail_open treats the stock as clean,
    fail_closed treats it as blocked; either way a warning is recorded.
    """
    if value is None:
        warnings.append(unavailable_warning)
        return policy == POLICY_FAIL_CLOSED, False
    return bool(value), True


def evaluate_exclusions(
    entry: UniverseEntry,
    snapshot: PreCloseSnapshot | None,
    config: SelectorConfig,
    warnings: list[str],
) -> dict[str, object]:
    """Effective exclusion flags plus per-dataset availability."""
    tradable = entry.tradable if entry.tradable is not None else True
    if entry.tradable is None:
        warnings.append('TRADABILITY_DATA_UNAVAILABLE')

    btst_blocked, btst_available = resolve_optional_flag(
        None if entry.btst_eligible is None else not entry.btst_eligible,
        config.btst_eligibility_missing_policy,
        warnings,
        'BTST_ELIGIBILITY_DATA_UNAVAILABLE',
    )
    restricted, restriction_available = resolve_optional_flag(
        entry.restricted_security,
        config.restriction_data_missing_policy,
        warnings,
        'RESTRICTION_DATA_UNAVAILABLE',
    )
    event_risk, event_available = resolve_optional_flag(
        entry.event_risk,
        config.event_data_missing_policy,
        warnings,
        'EVENT_DATA_UNAVAILABLE',
    )
    corporate_action_risk, corp_available = resolve_optional_flag(
        entry.corporate_action_risk,
        config.event_data_missing_policy,
        warnings,
        'CORPORATE_ACTION_DATA_UNAVAILABLE',
    )
    ex_date_risk, _ = resolve_optional_flag(
        entry.ex_date_next_session,
        config.event_data_missing_policy,
        warnings,
        'EX_DATE_DATA_UNAVAILABLE',
    )

    if snapshot is not None and snapshot.circuit_locked is not None:
        circuit_locked = bool(snapshot.circuit_locked)
        circuit_available = True
    else:
        circuit_locked = False
        circuit_available = False
        if config.exclude_circuit_locked_securities:
            warnings.append('CIRCUIT_LOCK_DATA_UNAVAILABLE')

    return {
        'tradable': bool(tradable),
        'btst_eligible': not btst_blocked,
        'btst_data_available': btst_available,
        'restricted_security': restricted,
        'restriction_data_available': restriction_available,
        'event_risk': bool(event_risk or ex_date_risk),
        'event_data_available': event_available,
        'corporate_action_risk': corporate_action_risk,
        'corporate_action_data_available': corp_available,
        'circuit_locked': circuit_locked,
        'circuit_data_available': circuit_available,
    }
