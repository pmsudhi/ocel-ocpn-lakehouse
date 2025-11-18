import json
from pathlib import Path
from datetime import datetime
import daft
from pyiceberg.catalog import load_catalog


def _load_catalog(catalog_name: str, catalog_cfg: Path):
    cfg = {}
    with catalog_cfg.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                k, v = line.split(':', 1)
                cfg[k.strip()] = v.strip()
    return load_catalog(catalog_name, **cfg)


def _explode_event(event: dict):
    # Returns tuple: (event_row, rel_rows, attr_rows)
    time_str = event['time']
    ts = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    event_date = ts.date().isoformat()
    event_month = ts.strftime('%Y-%m')

    vendor_code = None
    request_id = None
    for attr in event.get('attributes', []):
        if attr.get('name') in ('orderVendorCode', 'vendorCode'):
            vendor_code = attr.get('value')
        if attr.get('name') in ('requestId', 'instanceId'):
            request_id = attr.get('value')

    event_row = {
        'id': event['id'],
        'type': event['type'],
        'time': ts.isoformat(),
        'event_date': event_date,
        'event_month': event_month,
        'vendor_code': vendor_code,
        'request_id': request_id,
    }

    rel_rows = []
    for rel in event.get('relationships', []):
        rel_rows.append({
            'event_id': event['id'],
            'object_id': rel.get('objectId'),
            'qualifier': rel.get('qualifier')
        })

    attr_rows = []
    for attr in event.get('attributes', []):
        name = attr.get('name')
        val = attr.get('value')
        val_type = None
        row = {'event_id': event['id'], 'name': name}
        if isinstance(val, bool):
            row['val_boolean'] = val
            val_type = 'boolean'
        elif isinstance(val, int):
            row['val_long'] = val
            val_type = 'long'
        elif isinstance(val, float):
            row['val_double'] = val
            val_type = 'double'
        else:
            # try timestamp parse
            try:
                _ = datetime.fromisoformat(str(val).replace('Z', '+00:00'))
                row['val_timestamp'] = str(val)
                val_type = 'timestamp'
            except Exception:
                row['val_string'] = str(val)
                val_type = 'string'
        row['val_type'] = val_type
        attr_rows.append(row)

    return event_row, rel_rows, attr_rows


def load_ocel_to_iceberg(ocel_path: Path, catalog_cfg: Path):
    with ocel_path.open('r', encoding='utf-8') as f:
        ocel = json.load(f)

    events = ocel.get('events', [])
    objects = ocel.get('objects', [])

    event_rows = []
    rel_rows = []
    attr_rows = []

    for ev in events:
        e, r, a = _explode_event(ev)
        event_rows.append(e)
        rel_rows.extend(r)
        attr_rows.extend(a)

    # objects
    object_rows = []
    object_attr_rows = []
    for obj in objects:
        o = {
            'id': obj['id'],
            'type': obj['type'],
            'created_at': None,
            'lifecycle_state': None,
        }
        object_rows.append(o)
        for attr in obj.get('attributes', []):
            name = attr.get('name')
            val = attr.get('value')
            row = {'object_id': obj['id'], 'name': name}
            if isinstance(val, bool):
                row['val_boolean'] = val
                row['val_type'] = 'boolean'
            elif isinstance(val, int):
                row['val_long'] = val
                row['val_type'] = 'long'
            elif isinstance(val, float):
                row['val_double'] = val
                row['val_type'] = 'double'
            else:
                try:
                    _ = datetime.fromisoformat(str(val).replace('Z', '+00:00'))
                    row['val_timestamp'] = str(val)
                    row['val_type'] = 'timestamp'
                except Exception:
                    row['val_string'] = str(val)
                    row['val_type'] = 'string'
            object_attr_rows.append(row)

    cat = _load_catalog('local', catalog_cfg)

    # Resolve data files/locations via catalog, then write Parquet using Daft
    def to_daft(rows):
        if not rows:
            return None
        return daft.from_pydict({k: [r.get(k) for r in rows] for k in rows[0].keys()})

    datasets = {
        'ocel.events': to_daft(event_rows),
        'ocel.event_objects': to_daft(rel_rows),
        'ocel.event_attributes': to_daft(attr_rows),
        'ocel.objects': to_daft(object_rows),
        'ocel.object_attributes': to_daft(object_attr_rows),
    }

    for table_name, df in datasets.items():
        if df is None:
            continue
        table = cat.load_table(table_name)
        # Write out as Parquet into table's location using Daft; rely on Iceberg add_files via append
        # For simplicity in this first cut, write partitioned by columns present, then call refresh on catalog
        table_location = str(table.location())
        if table_location.startswith('file://'):
            table_location = table_location[7:]  # Remove file:// prefix
        out_dir = Path(table_location) / 'staged-load'
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(out_dir / f"{table_name.replace('.', '_')}_{datetime.utcnow().timestamp()}.parquet")
        df.write_parquet(output_path)
        table.refresh()
    print("Daft batch load completed (files written). Integrating files with Iceberg commit can be added next.")


if __name__ == '__main__':
    ocel_file = Path('data/purchase_to_pay_ocel_v2.json').resolve()
    catalog_file = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'
    load_ocel_to_iceberg(ocel_file, catalog_file)

