window.BENCHMARK_DATA = {
  "lastUpdate": 1791371458619,
  "repoUrl": "https://github.com/simongdavies/hyperlight-unikraft",
  "entries": {
    "python benchmarks": [
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
        "date": 1791371456603,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "cold/compute",
            "value": 343.815,
            "unit": "ms"
          },
          {
            "name": "cold/hello",
            "value": 345.804,
            "unit": "ms"
          },
          {
            "name": "cold/mount",
            "value": 351.794,
            "unit": "ms"
          },
          {
            "name": "cold/stdlib",
            "value": 403.327,
            "unit": "ms"
          },
          {
            "name": "cold-snap/compute",
            "value": 36.47,
            "unit": "ms"
          },
          {
            "name": "cold-snap/hello",
            "value": 22.101,
            "unit": "ms"
          },
          {
            "name": "cold-snap/mount",
            "value": 32.608,
            "unit": "ms"
          },
          {
            "name": "cold-snap/stdlib",
            "value": 103.587,
            "unit": "ms"
          },
          {
            "name": "warm-restore/compute",
            "value": 10.692,
            "unit": "ms"
          },
          {
            "name": "warm-restore/hello",
            "value": 3.838,
            "unit": "ms"
          },
          {
            "name": "warm-restore/mount",
            "value": 7.639,
            "unit": "ms"
          },
          {
            "name": "warm-restore/stdlib",
            "value": 45.657,
            "unit": "ms"
          },
          {
            "name": "restore-cost/compute",
            "value": 6.975,
            "unit": "ms"
          },
          {
            "name": "restore-cost/hello",
            "value": 6.601,
            "unit": "ms"
          },
          {
            "name": "restore-cost/mount",
            "value": 7.9,
            "unit": "ms"
          },
          {
            "name": "restore-cost/stdlib",
            "value": 7.491,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/compute",
            "value": 1.65,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/hello",
            "value": 0.199,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/mount",
            "value": 1.519,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/stdlib",
            "value": 22.517,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/compute",
            "value": 14.336,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/hello",
            "value": 5.118,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/mount",
            "value": 10.412,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/stdlib",
            "value": 71.452,
            "unit": "ms"
          },
          {
            "name": "snapshot-size/compute",
            "value": 73.6,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/hello",
            "value": 73.6,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/mount",
            "value": 73.6,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/stdlib",
            "value": 73.6,
            "unit": "MiB"
          },
          {
            "name": "rss/compute",
            "value": 14,
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
            "value": 21,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}