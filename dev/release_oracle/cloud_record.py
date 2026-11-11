"""Record the cloud flow once against the real test project: recordings of the stores' and the warehouse's answers, every credential masked."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from . import cloud_legs, cloud_replay as cr, gcp


def main() -> int:
    """Run the live flow through recording proxies, write dev/cloud_legs/recordings, and print the cloud tier's verdict lines."""
    if not cloud_legs.cloud_env():
        print("cloud_record: BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET and BIGQUERY_TEST_LOCATION must be set; nothing recorded", file=sys.stderr)
        return 2
    secrets = (gcp.token(),)
    with cr.Recorder("storage.googleapis.com", secrets) as gcs, cr.Recorder("bigquery.googleapis.com", secrets) as bq:
        flow = cloud_legs.live_flow(Path(tempfile.gettempdir()) / "cloud_record", {"gcs": gcs, "bq": bq})
    for prop, rs in cloud_legs.grade("cloud", flow).items():
        print("\n".join(cloud_legs.verdict_line(prop, r) for r in sorted(rs, key=lambda r: r.cell)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
