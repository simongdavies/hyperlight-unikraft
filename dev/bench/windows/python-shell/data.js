window.BENCHMARK_DATA = {
  "lastUpdate": 1791371597559,
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
        "date": 1791371591985,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "cold/compute",
            "value": 889.402,
            "unit": "ms"
          },
          {
            "name": "cold/hello",
            "value": 885.46,
            "unit": "ms"
          },
          {
            "name": "cold/mount",
            "value": 889.862,
            "unit": "ms"
          },
          {
            "name": "cold/stdlib",
            "value": 899.887,
            "unit": "ms"
          },
          {
            "name": "cold-snap/compute",
            "value": 55.213,
            "unit": "ms"
          },
          {
            "name": "cold-snap/hello",
            "value": 32.967,
            "unit": "ms"
          },
          {
            "name": "cold-snap/mount",
            "value": 49.521,
            "unit": "ms"
          },
          {
            "name": "cold-snap/stdlib",
            "value": 89.063,
            "unit": "ms"
          },
          {
            "name": "warm-restore/compute",
            "value": 16.772,
            "unit": "ms"
          },
          {
            "name": "warm-restore/hello",
            "value": 5.72,
            "unit": "ms"
          },
          {
            "name": "warm-restore/mount",
            "value": 11.512,
            "unit": "ms"
          },
          {
            "name": "warm-restore/stdlib",
            "value": 32.549,
            "unit": "ms"
          },
          {
            "name": "restore-cost/compute",
            "value": 11.818,
            "unit": "ms"
          },
          {
            "name": "restore-cost/hello",
            "value": 11.604,
            "unit": "ms"
          },
          {
            "name": "restore-cost/mount",
            "value": 12.954,
            "unit": "ms"
          },
          {
            "name": "restore-cost/stdlib",
            "value": 11.976,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/compute",
            "value": 2.898,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/hello",
            "value": 0.309,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/mount",
            "value": 2.134,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/stdlib",
            "value": 11.317,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/compute",
            "value": 24.324,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/hello",
            "value": 8.095,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/mount",
            "value": 15.567,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/stdlib",
            "value": 45.127,
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
            "value": 15,
            "unit": "MB"
          },
          {
            "name": "rss/hello",
            "value": 12,
            "unit": "MB"
          },
          {
            "name": "rss/mount",
            "value": 14,
            "unit": "MB"
          },
          {
            "name": "rss/stdlib",
            "value": 18,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}