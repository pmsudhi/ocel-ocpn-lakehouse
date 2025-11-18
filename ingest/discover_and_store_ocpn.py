from pathlib import Path
from datetime import datetime
import json


def discover_ocpn_from_ocel(ocel_json_path: Path):
    try:
        import pm4py
        # pm4py >=2.7 reading OCEL 2.0 JSON
        try:
            ocel = pm4py.read_ocel2_json(str(ocel_json_path))
        except Exception:
            # fallback generic reader
            ocel = pm4py.read_ocel(str(ocel_json_path))

        # Try discovery; API may vary by pm4py version
        try:
            from pm4py.algo.discovery.ocel.ocpn import algorithm as ocpn_discovery
            ocpn = ocpn_discovery.apply(ocel)
        except Exception:
            # fallback simple model discovery if available
            from pm4py.discovery import discover_petri_net_alpha
            # This is not object-centric; only as last resort
            events = ocel.get("events", []) if isinstance(ocel, dict) else []
            log = []
            for ev in events:
                log.append([ev.get("type", "event")])
            net, im, fm = discover_petri_net_alpha(log)
            ocpn = {"net": net, "im": im, "fm": fm}
        return ocpn
    except Exception as e:
        print(f"PM4Py discovery failed: {e}")
        return None


def parse_pnml_to_tables(pnml_path: Path, model_id: str):
    # Minimal PNML parser using xml.etree for places/transitions/arcs
    import xml.etree.ElementTree as ET
    ns = {'pnml': 'http://www.pnml.org/version-2009/grammar/pnml'}
    tree = ET.parse(str(pnml_path))
    root = tree.getroot()
    places, transitions, arcs = [], [], []
    for p in root.findall('.//pnml:place', ns):
        pid = p.attrib.get('id')
        label_el = p.find('.//pnml:text', ns)
        label = label_el.text if label_el is not None else None
        places.append({'model_id': model_id, 'place_id': pid, 'label': label})
    for t in root.findall('.//pnml:transition', ns):
        tid = t.attrib.get('id')
        label_el = t.find('.//pnml:text', ns)
        label = label_el.text if label_el is not None else None
        transitions.append({'model_id': model_id, 'transition_id': tid, 'label': label, 'invisible': False})
    for a in root.findall('.//pnml:arc', ns):
        aid = a.attrib.get('id')
        src = a.attrib.get('source')
        dst = a.attrib.get('target')
        arcs.append({'model_id': model_id, 'arc_id': aid, 'src_type': 'unknown', 'src_id': src, 'dst_type': 'unknown', 'dst_id': dst, 'weight': 1})
    return places, transitions, arcs


def write_ocpn_tables_with_daft(model_id: str, model_name: str, pnml_path: Path, catalog_cfg: Path):
    import daft
    from pyiceberg.catalog import load_catalog

    places, transitions, arcs = parse_pnml_to_tables(pnml_path, model_id)
    models = [{
        'model_id': model_id,
        'version': 1,
        'name': model_name,
        'created_at': datetime.utcnow().isoformat(),
        'source_format': 'PNML',
        'raw_pnml': None,
        'notes': None,
    }]

    cfg = {}
    with catalog_cfg.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                k, v = line.split(':', 1)
                cfg[k.strip()] = v.strip()
    cat = load_catalog('local', **cfg)

    def to_daft(rows):
        if not rows:
            return None
        return daft.from_pydict({k: [r.get(k) for r in rows] for k in rows[0].keys()})

    datasets = {
        'ocpn.models': to_daft(models),
        'ocpn.places': to_daft(places),
        'ocpn.transitions': to_daft(transitions),
        'ocpn.arcs': to_daft(arcs),
    }

    for table_name, df in datasets.items():
        if df is None:
            continue
        table = cat.load_table(table_name)
        table_location = str(table.location())
        if table_location.startswith('file://'):
            table_location = table_location[7:]  # Remove file:// prefix
        out_dir = Path(table_location) / 'staged-load'
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(out_dir / f"{table_name.replace('.', '_')}_{datetime.utcnow().timestamp()}.parquet")
        df.write_parquet(output_path)
        table.refresh()
    print("OCPN tables written as Parquet (staged). Next step: integrate with Iceberg commits.")


def main():
    ocel_path = Path('data/purchase_to_pay_ocel_v2.json').resolve()
    catalog_cfg = Path(__file__).parents[1] / 'catalogs' / 'local.yaml'

    model_id = f"ocpn_{int(datetime.utcnow().timestamp())}"
    model_name = 'p2p_discovered'

    # Use existing PNML file
    pnml_fallback = Path('lakehouse/ingest/simple_test.pnml')
    
    if pnml_fallback.exists():
        print(f"Using existing PNML: {pnml_fallback}")
        write_ocpn_tables_with_daft(model_id, model_name, pnml_fallback, catalog_cfg)
    else:
        print("No PNML available to store OCPN tables.")


if __name__ == '__main__':
    main()

