# Managed / Proactive Strategy Template

This folder shows how to write a **proactive** strategy that pre-places orders and maintains them.

- Set `"runner_type": "managed"` in your config.
- Implement `desired_actions(prices, open_orders, state, now_ts) -> List[OrderAction]`

The generic **managed runner** supports:
- PLACE (market/limit) and CANCEL
- Adaptive qty reduction on SELL rejects (common execution layer)

For sophisticated strategies (order modifications, multi-leg, etc.),
you can still keep a **custom runner** inside the strategy folder,
but reuse `common/broker/` and `common/engine/execution.py`.
