window.BENCHMARK_DATA = {
  "lastUpdate": 1791371350639,
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
        "date": 1791371349701,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "cold/compute",
            "value": 140.562,
            "unit": "ms"
          },
          {
            "name": "cold/hello",
            "value": 141.479,
            "unit": "ms"
          },
          {
            "name": "cold/mount",
            "value": 143.193,
            "unit": "ms"
          },
          {
            "name": "cold/stdlib",
            "value": 169.294,
            "unit": "ms"
          },
          {
            "name": "cold-snap/compute",
            "value": 11.918,
            "unit": "ms"
          },
          {
            "name": "cold-snap/hello",
            "value": 7.097,
            "unit": "ms"
          },
          {
            "name": "cold-snap/mount",
            "value": 10.227,
            "unit": "ms"
          },
          {
            "name": "cold-snap/stdlib",
            "value": 46.537,
            "unit": "ms"
          },
          {
            "name": "warm-restore/compute",
            "value": 4.014,
            "unit": "ms"
          },
          {
            "name": "warm-restore/hello",
            "value": 1.191,
            "unit": "ms"
          },
          {
            "name": "warm-restore/mount",
            "value": 2.294,
            "unit": "ms"
          },
          {
            "name": "warm-restore/stdlib",
            "value": 29.812,
            "unit": "ms"
          },
          {
            "name": "restore-cost/compute",
            "value": 0.887,
            "unit": "ms"
          },
          {
            "name": "restore-cost/hello",
            "value": 0.896,
            "unit": "ms"
          },
          {
            "name": "restore-cost/mount",
            "value": 1.195,
            "unit": "ms"
          },
          {
            "name": "restore-cost/stdlib",
            "value": 1.118,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/compute",
            "value": 1.458,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/hello",
            "value": 0.09,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/mount",
            "value": 0.431,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/stdlib",
            "value": 22.117,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/compute",
            "value": 5.997,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/hello",
            "value": 1.621,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/mount",
            "value": 3.168,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/stdlib",
            "value": 51.664,
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
            "value": 8,
            "unit": "MB"
          },
          {
            "name": "rss/hello",
            "value": 6,
            "unit": "MB"
          },
          {
            "name": "rss/mount",
            "value": 6,
            "unit": "MB"
          },
          {
            "name": "rss/stdlib",
            "value": 7,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}