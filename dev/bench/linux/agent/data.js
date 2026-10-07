window.BENCHMARK_DATA = {
  "lastUpdate": 1791371873337,
  "repoUrl": "https://github.com/simongdavies/hyperlight-unikraft",
  "entries": {
    "agent benchmarks": [
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
        "date": 1791371872653,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "cold/compute",
            "value": 6251.389,
            "unit": "ms"
          },
          {
            "name": "cold/hello",
            "value": 6241.209,
            "unit": "ms"
          },
          {
            "name": "cold/mount",
            "value": 6252.777,
            "unit": "ms"
          },
          {
            "name": "cold/stdlib",
            "value": 6251.76,
            "unit": "ms"
          },
          {
            "name": "cold-snap/compute",
            "value": 17.939,
            "unit": "ms"
          },
          {
            "name": "cold-snap/hello",
            "value": 9.55,
            "unit": "ms"
          },
          {
            "name": "cold-snap/mount",
            "value": 15.521,
            "unit": "ms"
          },
          {
            "name": "cold-snap/stdlib",
            "value": 36.064,
            "unit": "ms"
          },
          {
            "name": "warm-restore/compute",
            "value": 7.07,
            "unit": "ms"
          },
          {
            "name": "warm-restore/hello",
            "value": 1.873,
            "unit": "ms"
          },
          {
            "name": "warm-restore/mount",
            "value": 3.854,
            "unit": "ms"
          },
          {
            "name": "warm-restore/stdlib",
            "value": 18.374,
            "unit": "ms"
          },
          {
            "name": "restore-cost/compute",
            "value": 1.588,
            "unit": "ms"
          },
          {
            "name": "restore-cost/hello",
            "value": 1.431,
            "unit": "ms"
          },
          {
            "name": "restore-cost/mount",
            "value": 2.262,
            "unit": "ms"
          },
          {
            "name": "restore-cost/stdlib",
            "value": 1.749,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/compute",
            "value": 2.545,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/hello",
            "value": 0.169,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/mount",
            "value": 0.989,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/stdlib",
            "value": 10.269,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/compute",
            "value": 11.8,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/hello",
            "value": 3.134,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/mount",
            "value": 6.23,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/stdlib",
            "value": 26.467,
            "unit": "ms"
          },
          {
            "name": "snapshot-size/compute",
            "value": 822.8,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/hello",
            "value": 822.8,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/mount",
            "value": 822.8,
            "unit": "MiB"
          },
          {
            "name": "snapshot-size/stdlib",
            "value": 822.8,
            "unit": "MiB"
          },
          {
            "name": "rss/compute",
            "value": 9,
            "unit": "MB"
          },
          {
            "name": "rss/hello",
            "value": 7,
            "unit": "MB"
          },
          {
            "name": "rss/mount",
            "value": 9,
            "unit": "MB"
          },
          {
            "name": "rss/stdlib",
            "value": 11,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}