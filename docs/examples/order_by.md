Row Order
---

Cross-table operators need an explicit row order: `view(order_by=...)` on both sides, and the `TypeError` without it. Run it with `python -m aaiclick.data.examples.order_by`.

```python
--8<-- "aaiclick/data/examples/order_by.py"
```
