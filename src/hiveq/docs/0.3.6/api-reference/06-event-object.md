## 6. The Event object

```
event.type        -> EventType          # branch on this
event.data()      -> payload object     # type depends on event.type (§7.0)
event.ts_event    -> int                # nanoseconds
event.time        -> Optional[datetime] # configured tz
event.time_utc    -> Optional[datetime] # UTC
```

---

