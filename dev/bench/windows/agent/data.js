window.BENCHMARK_DATA = {
  "lastUpdate": 1791372529089,
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
        "date": 1791372521969,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "cold/compute",
            "value": 12040.685,
            "unit": "ms"
          },
          {
            "name": "cold/hello",
            "value": 11932.025,
            "unit": "ms"
          },
          {
            "name": "cold/mount",
            "value": 12135.443,
            "unit": "ms"
          },
          {
            "name": "cold/stdlib",
            "value": 12246.294,
            "unit": "ms"
          },
          {
            "name": "cold-snap/compute",
            "value": 97.618,
            "unit": "ms"
          },
          {
            "name": "cold-snap/hello",
            "value": 67.193,
            "unit": "ms"
          },
          {
            "name": "cold-snap/mount",
            "value": 97.524,
            "unit": "ms"
          },
          {
            "name": "cold-snap/stdlib",
            "value": 156.773,
            "unit": "ms"
          },
          {
            "name": "warm-restore/compute",
            "value": 21.569,
            "unit": "ms"
          },
          {
            "name": "warm-restore/hello",
            "value": 7.785,
            "unit": "ms"
          },
          {
            "name": "warm-restore/mount",
            "value": 16.516,
            "unit": "ms"
          },
          {
            "name": "warm-restore/stdlib",
            "value": 45.492,
            "unit": "ms"
          },
          {
            "name": "restore-cost/compute",
            "value": 42.599,
            "unit": "ms"
          },
          {
            "name": "restore-cost/hello",
            "value": 43.209,
            "unit": "ms"
          },
          {
            "name": "restore-cost/mount",
            "value": 46.023,
            "unit": "ms"
          },
          {
            "name": "restore-cost/stdlib",
            "value": 47.005,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/compute",
            "value": 2.422,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/hello",
            "value": 0.339,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/mount",
            "value": 3.015,
            "unit": "ms"
          },
          {
            "name": "warm-stateful/stdlib",
            "value": 10.426,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/compute",
            "value": 25.517,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/hello",
            "value": 9.338,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/mount",
            "value": 19.758,
            "unit": "ms"
          },
          {
            "name": "parallel-exec/stdlib",
            "value": 82.502,
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
            "value": 23,
            "unit": "MB"
          },
          {
            "name": "rss/hello",
            "value": 20,
            "unit": "MB"
          },
          {
            "name": "rss/mount",
            "value": 24,
            "unit": "MB"
          },
          {
            "name": "rss/stdlib",
            "value": 27,
            "unit": "MB"
          }
        ]
      }
    ]
  }
}