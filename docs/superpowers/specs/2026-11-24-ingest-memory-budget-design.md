# Ingest memory budget: design

Status: approved 2026-11-24.

## Why

Ingest holds each file whole in memory: a worker reads it, hashes it to see
whether the repository already has it, and compresses it, and a new file
then waits whole in the storage queue (4 messages per worker). In the worst
case that is about 6 x workers x file size. On the real archive, 4 workers
peaked at 16.6 GB and 8 workers at 20.5 GB on video. The default of one
worker per CPU can run a smaller machine out of memory, and an ingest that
dies has to be started over, because `recover` does not exist yet.

## Goals

- Peak file data held by ingest is bounded by a budget, whatever the worker
  count and file sizes.
- The default budget is a quarter of physical RAM. `wmk ingest
  --max-memory <SIZE>` overrides it.
- A file larger than the whole budget still ingests, alone.
- Ingest output is unchanged: the same snapshot, catalog and packs.
- Little or no slowdown when files are small relative to the budget.

## Non-goals

- Streaming large files. That is Plan 3, which removes whole-file buffering.
- Bounding memory outside file data: the catalog, the unit cache, the walk's
  sets, allocator overhead.
- Container memory limits (cgroups). The default reads physical RAM only;
  `--max-memory` covers containers.

## Anti-goals

- A deadlock or hang: when the pipeline aborts, a worker waiting on the
  budget must wake up and stop.
- Any change to what is stored or to the ingest guarantees.

## Approaches considered

1. **A byte budget on file data in flight (chosen).** Every file reserves
   its size before it is read and releases it when its data is gone.
2. **Cap the worker count by RAM.** Rejected: it ignores file size, so it
   either wastes cores on small files or still overflows on large ones.
3. **Stream large files now.** Rejected for now: it is Plan 3's two-pass
   read, a much larger change.

## Design

- **Budget.** A counting semaphore in bytes, shared by the workers. A
  worker reserves the file's size, from the fstat before the read, before
  reading it. The reservation is a permit that releases its bytes when it
  is dropped.
- **Where the permit goes.** If the worker emits a `NewContent` message,
  the permit travels inside it and is dropped once storage has appended the
  units. Otherwise (known content, or content another worker claimed) it is
  dropped when the worker finishes the file. Dropping on any path,
  including errors and aborts, releases the bytes.
- **Order.** Requests are granted first come, first served, so a large file
  cannot be starved by a stream of small ones.
- **Oversize files.** A request larger than the budget is granted when
  nothing else is held, and then holds the whole budget until it is done.
  So peak held bytes are at most the larger of the budget and the largest
  file.
- **Growth on re-read.** If the unstable-file retry reads more bytes than
  were reserved, the permit grows to the bytes read without waiting: a
  rare, bounded overshoot instead of a possible deadlock.
- **Abort.** A waiting worker wakes and stops when the pipeline aborts.
- **Default.** A quarter of physical RAM: `sysctlbyname("hw.memsize")` on
  macOS, `sysconf(_SC_PHYS_PAGES) x _SC_PAGESIZE` on Linux, through `libc`,
  which is already a dependency. If detection fails, 4 GiB.
- **API.** `IngestOptions` gains `max_memory: Option<u64>`; `None` means the
  default. The CLI gains `--max-memory <SIZE>`, taking bytes or a binary
  suffix (`512M`, `8G`); zero and unparseable values are errors. The help
  text says it bounds buffered file data, not the whole process.

## Testing

- Budget unit tests: a request over the free bytes waits; release wakes
  waiters in order; an oversize request waits for an empty budget and runs
  alone; abort wakes a waiter.
- Ingest with a small budget, many workers, and files both smaller and
  larger than the budget: the peak held bytes, read from a test seam, stay
  within the larger of the budget and the largest file, and the round trip
  is exact.
- An abort while workers wait on the budget returns an error and does not
  hang.
- CLI: `--max-memory` parses suffixes and rejects zero and junk.
- Measured on this Mac with synthetic large files: peak RSS with the default
  budget and with a small one, against the current code.

## Left to the builder

The permit and semaphore types and where they live; how the test seam
exposes peak held bytes; the exact size-suffix grammar beyond the examples;
whether the measurement uses `/usr/bin/time -l` or another tool.

## Results

Measured on 2026-11-24 on a 14-core laptop with 36 GiB of RAM (so the
default budget is 9 GiB), release builds, warm page cache. Source: 8
incompressible files of 512 MiB plus 2,000 incompressible files of 64 KiB,
4.43 GB in all; `wmk ingest --workers 8` into a fresh repository each time.
Two rounds per row; figures from `/usr/bin/time -l`.

| Build | Max RSS | Wall |
| --- | --- | --- |
| 0.1.2 (`main`, no budget) | 4.27, 4.28 GiB | 3.65, 3.39 s |
| Branch, default budget (9 GiB) | 4.28, 4.28 GiB | 3.40, 3.39 s |
| Branch, `--max-memory 1G` | 1.70, 1.79 GiB | 4.52, 4.59 s |

- **Default budget:** no change here, as intended: the whole data set
  (4.4 GB) fits in a quarter of this machine's RAM. The budget bites on
  machines where the files in flight exceed a quarter of RAM.
- **Small budget:** peak RSS fell from 4.3 GiB to about 1.75 GiB, at about
  1.75x the budget rather than near it. The cause was not isolated. The
  budget counts each file's size once, and a worker likely holds more for
  a moment: on data zstd cannot shrink, the compression attempt's output
  buffer sits beside the raw chunks, and the reader keeps its own buffer.
  So, per the design, `--max-memory` bounds file data, not the process;
  plan for up to about twice the budget on incompressible data.
- **Speed:** with 1 GiB, at most two 512 MiB files are in flight, so the
  run took about 33% longer (4.5 s against 3.4 s).
- The real archive (16.6 GB peak with 4 workers) was not re-measured.
