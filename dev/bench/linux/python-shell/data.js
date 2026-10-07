window.BENCHMARK_DATA = {
  "lastUpdate": 1791371403779,
  "repoUrl": "https://github.com/simongdavies/hyperlight-unikraft",
  "entries": {
    "python-shell benchmarks": [
      {
        "commit": {
          "author": {
            "email": "danilochiarlone@gmail.com",
            "name": "danbugs",
            "username": "danbugs"
          },
          "committer": {
            "email": "danilochiarlone@gmail.com",
            "name": "danbugs",
            "username": "danbugs"
          },
          "distinct": false,
          "id": "3df47f64f99229e3cebef07b22ba948c69e1398c",
          "message": "site: stop narrow phones from scrolling sideways\n\nAt 375px the template picker made the page 4px wider than the screen,\nand at 320px the platform table did, by 11px. The template grid's one\ncolumn can now shrink below its content, and below 340px the table\nbreaks \"Hypervisor.framework\" rather than widening the page.\n\nSigned-off-by: danbugs <danilochiarlone@gmail.com>",
          "timestamp": "2026-10-02T23:41:32Z",
          "tree_id": "07515b9b26e279dec1882c4b82f82d6b214986af",
          "url": "https://github.com/simongdavies/hyperlight-unikraft/commit/3df47f64f99229e3cebef07b22ba948c69e1398c"
        },
        "date": 1791371402725,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "cold/compute",
            "value": 444.026,
            "unit": "ms"
          },
          {
            "name": "cold/hello",
            "value": 438.605,
            "unit": "ms"
          },
          {
            "name": "cold/mount",
            "value": 442.917,
            "unit": "ms"
          },
          {
            "name": "cold/stdlib",
            "value": 449.088,
            "unit": "ms"
          },
          {
            "name": "cold-snap/compute",
            "value": 16.313,
            "unit": "ms"
          },
          {
            "name": "cold-snap/hello",
            "value": 8.462,
            "unit": "ms"
          },
          {
            "name": "cold-snap/mount",
            "value": 13.321,
            "unit": "ms"
          },
          {
            "name": "cold-snap/stdlib",
            "value": 31.525,
            "unit": "ms"
          },
          {
            "name": "warm-restore/compute",
            "value": 6.281,
            "unit": "ms"
          },
          {
            "name": "warm-restore/hello",
            "value": 1.62,
            "unit": "ms"
          },
          {
            "name": "warm-restore/mount",
            "value": 3.253,
            "unit": "ms"
          },
          {
            "name": "warm-restore/stdlib",
            "value": 16.303,
            "unit": "ms"
          },
          {
            "name": "restore-cost/compute",
            "value": 1.008,
            "unit": "ms"
          },
          {
            "name": "restore-cost/hello",
            "value": 0.936,
            "unit": "ms"
          },
          {
            "name": "restore-cost/mount",
            "value": 1.391,
            "unit": "ms"
          },
          {
            "name": "restore-cost/stdlib",
            "value": 1.078,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/compute",
            "value": 2.633,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/hello",
            "value": 0.216,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/mount",
            "value": 0.693,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/stdlib",
            "value": 10.507,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/compute",
            "value": 10.425,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/hello",
            "value": 2.499,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/mount",
            "value": 4.925,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/stdlib",
            "value": 22.901,
            "unit": "ms"
          },
          {
            "name": "snapshot-size/compute",
            "value": 102.5,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/hello",
            "value": 102.5,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/mount",
            "value": 102.5,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/stdlib",
            "value": 102.5,
            "unit": "MiB"
          },
          {
            "name": "rss/compute",
            "value": 7,
            "unit": "MB"
          },
          {
            "name": "rss/hello",
            "value": 7,
            "unit": "MB"
          },
          {
            "name": "rss/mount",
            "value": 7,
            "unit": "MB"
          },
          {
            "name": "rss/stdlib",
            "value": 8,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}