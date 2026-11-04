"""Read-only fresh-process verification for a synthetic local Silver catalog."""

from __future__ import annotations

import json
import sys

from pyiceberg.catalog.sql import SqlCatalog


def main() -> int:
    catalog_uri, warehouse, event_id = sys.argv[1:4]
    catalog = SqlCatalog("registered-test", uri=catalog_uri, warehouse=warehouse)
    table = catalog.load_table("synthetic.silver")
    facts = table.scan().to_arrow().to_pylist()
    matching = [fact for fact in facts if fact["event_id"] == event_id]
    if len(matching) != 1:
        raise AssertionError(f"expected one committed fact, got {len(matching)}")
    print(
        json.dumps(
            {
                "snapshot_id": table.current_snapshot().snapshot_id,
                "fact_count": len(matching),
                "lineage_ref": matching[0]["lineage_ref"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
