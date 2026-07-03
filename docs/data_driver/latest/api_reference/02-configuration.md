## Configuration

The driver needs **config** to map a `data_source_id` to a transport — that is
the only thing it can't infer. The config is the same two-level
`{section: {property: value}}` shape however you supply it:

```ini
[AaplBars]
primary = HiveQBars1m

[HiveQBars1m]
transport = HiveQ
dataset   = HIVEQ_US_EQ
schema    = bars_1m
```

You can provide it any of these ways — pick whichever fits how you run:

- **Inline / programmatic** — pass a dict, or a path to a `.json` / `.py`
  (defining `CONFIG`) / `.ini` file, straight to the driver. Nothing on disk
  needs to be in a special place:
  ```python
  import hiveq.driver as dd
  dd.init(config={'AaplBars': {'primary': 'HiveQBars1m'},
                  'HiveQBars1m': {'transport': 'HiveQ',
                                  'dataset': 'HIVEQ_US_EQ', 'schema': 'bars_1m'}})
  # or: dd.init(config='dd-config.json')  /  dd.init(config='strategy_config.py')
  ```
- **Auto-discovered `dd-config.ini`** — if you pass no `config`, the driver looks
  for a `dd-config.ini` in the current working directory, then walks up the
  parent directories. This is just the zero-argument convenience; you are not
  required to use an ini.

If no config is supplied and none is found, the package still imports — calls
simply have nothing to resolve until you configure one.

**Note:** ini config supports multi-line entries; indent continuation lines.

**Credentials & endpoint.** The driver itself needs none — CSV/HDF5/KDB use no
key. A key is required **only** for `transport=HiveQ`, and only when such a section
is actually used. Put HiveQ-wide settings in a dedicated **`[HiveQ]`** section:

```ini
[HiveQ]
apiKey  = <your-key>             ; required for transport=HiveQ
baseUrl = https://vm.hiveq.ai    ; the Data API endpoint for your environment
```

- Section header is **`[HiveQ]`** (case-sensitive — capital H, Q); properties are
  **`apiKey`** and **`baseUrl`** (camelCase).
- Resolution is **data-source section → `[HiveQ]` → `[default]`**, so you can also
  set these on an individual source or in `[default]`; `[HiveQ]` is the recommended
  single place. The service derives the user/org from the key — no user/org ids are
  sent.

> ⚠️ The API key is a secret — keep it only in a **gitignored** config (the root
> `dd-config.ini` is gitignored), never in a tracked/committed one.

For the full HiveQ pull path (datasets/schemas, filter modes, pagination, the
config→SDK mapping), see [`hiveq_data_api_reference.md`](../hiveq_data_api_reference.md).

---

