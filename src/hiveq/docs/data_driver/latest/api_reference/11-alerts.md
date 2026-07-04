## Alerts

```python
import hiveq.driver as dd
dd.alert(subject='Test', message='Starting to train model')
```

`level` and `channel` are optional. Alerts are recorded via the standard logging
module (a Slack handler can be wired in via the `[slack]` extra).

---

