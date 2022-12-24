#include <skink/random.h>
#include <skink/zstream.h>

#include <algorithm>
#include <future>
#include <vector>

#include <sched.h>
#include <stdio.h>

#include <spdlog/spdlog.h>

#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>

// Returns current wall clock time, in seconds.
inline double stopwatch() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return tv.tv_sec + (double)tv.tv_nsec / 1e9;
}

// Returns time elapsed since a given start time, in seconds.
inline double stopwatch(double start) { return stopwatch() - start; }

// Writes a given amount of bytes to the stream, returns number written.
ssize_t writer(zstream &stream, ssize_t nbyte) {
  // writer thread goes on CPU 3
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(3, &cpus);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpus);

  std::vector<char> data(32768);

  ssize_t remain = nbyte;
  while (remain) {
    ssize_t nwrite = std::min(remain, (ssize_t)data.size());
    ssize_t nwrote = stream.write(data.data(), nwrite);
    remain -= nwrote;
    if (nwrote < nwrite) {
      break;
    }
  }
  stream.wrclose();
  return nbyte - remain;
}

// Reads a given amount of bytes from the stream, returns number read.
ssize_t reader(int id, zstream &stream, ssize_t nbyte) {
  // reader threads go on CPUs 4 on
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(4 + id, &cpus);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpus);

  std::vector<char> data(32768);

  ssize_t remain = nbyte;
  while (remain) {
    ssize_t size  = std::min(remain, (ssize_t)data.size());
    ssize_t nread = stream.read(id, data.data(), size, size);
    if (nread <= 0) {
      break;
    }
    remain -= nread;
  }
  stream.del_reader(id);
  return nbyte - remain;
}

// Writes a given amount of bytes to the stream, returns number written.
ssize_t writer_borrow(zstream &stream, ssize_t nbyte) {
  // writer thread goes on CPU 3
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(3, &cpus);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpus);

  ssize_t remain = nbyte;
  while (remain) {
    ssize_t nwrite = std::min(remain, (ssize_t)32768);

    void* ptr = stream.wborrow(nwrite);
    if (ptr == nullptr) {
      break;
    }
    stream.wrelease(nwrite);
    remain -= nwrite;

  }
  stream.wrclose();
  return nbyte - remain;
}

// Reads a given amount of bytes from the stream, returns number read.
ssize_t reader_borrow(int id, zstream &stream, ssize_t nbyte) {
  // reader threads go on CPUs 4 on
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(4 + id, &cpus);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpus);

  ssize_t remain = nbyte;
  while (remain) {
    ssize_t size  = std::min(remain, (ssize_t)32768);

    sizeptr<const void> ptr = stream.rborrow(id, size);
    if (!ptr) {
      break;
    }
    stream.rrelease(id, size);
    remain -= size;
  }
  stream.del_reader(id);
  return nbyte - remain;
}

static inline void benchmark(bool borrow) {
  constexpr ssize_t nbyte = 1ull << 34;  // 16GB

  if (borrow) {
    printf("# benchmarking borrow API\n");
  } else {
    printf("# benchmarking read/write API\n");
  }

  printf("# bufsize  act_size  nreader  write (B/s)  read (B/s)  passed?\n");
  for (int i = 12; i <= 23; ++i) {
    const ssize_t bufsize = 1ull << i;
    bool passed;
    for (ssize_t nreader = 1; nreader <= 8; ++nreader) {
      zstream stream(bufsize);
      stream.set_spin_limit(1000000);

      // Create readers.
      double tstart = stopwatch();
      std::vector<std::future<ssize_t>> results;
      for (ssize_t ii = 0; ii < nreader; ii++) {
        if (borrow) {
          results.emplace_back(std::async(std::launch::async,
              reader_borrow,
              stream.add_reader(),
              std::ref(stream),
              nbyte));
        } else {
          results.emplace_back(std::async(std::launch::async,
              reader,
              stream.add_reader(),
              std::ref(stream),
              nbyte));
        }
      }

      // Create writers.
      if (borrow) {
        std::future<ssize_t> resultw =
          std::async(std::launch::async, writer_borrow, std::ref(stream), nbyte);
      } else {
        std::future<ssize_t> resultw =
          std::async(std::launch::async, writer, std::ref(stream), nbyte);
      }

      passed = true;
      for (ssize_t ii = 0; ii < nreader; ii++) {
        passed &= (results[ii].get() == nbyte);
      }

      double telapsed = stopwatch(tstart);
      printf("%8zd %8zd %zd %.9e %.9e %d\n",
             bufsize,
             stream.size(),
             nreader,
             nbyte / telapsed,
             nreader * nbyte / telapsed,
             passed);
      fflush(stdout);
    }
  }
}

// Writes a given number of bytes to the stream as abusively as possible.
ssize_t writer_stress(zstream &stream, ssize_t nbyte) {
  std::vector<uint8_t> data(65534);
  prng rnd(1 << 16);

  ssize_t remain = nbyte;
  int iter = 0;
  uint8_t cnt = 0;
  while (remain) {
    ssize_t nwrite = std::min(remain, (ssize_t)rnd.uniform_i64(1, data.size()));

    if ((++iter) % 250 == 0) {
      fprintf(stderr, "\33[2K\rWriting %5zd, %zd remains", nwrite, remain);
    }

    for (int i=0; i < nwrite; ++i) {
      data[i] = cnt++;
    }

    ssize_t nwrote = stream.write(data.data(), nwrite);
    remain -= nwrote;
    if (nwrote < nwrite) {
      break;
    }
  }
  fprintf(stderr, "\33[2K\rDone writing, %zd remains\n", remain);
  stream.wrclose();
  return nbyte - remain;
}

// Reads a given amount of bytes from the stream as abusively as possible.
ssize_t reader_stress(int id, zstream &stream, ssize_t nbyte) {
  std::vector<uint8_t> data(32768);
  prng rnd(id+1);

  ssize_t remain = nbyte;
  uint8_t cnt = 0;
  bool passed = true;
  while (remain && passed) {
    ssize_t size  = std::min(remain, (ssize_t)rnd.uniform_i64(1, data.size()));
    ssize_t nread = stream.read(id, data.data(), size, size);
    if (nread <= 0) {
      break;
    }

    for (int i=0; i < nread; ++i) {
      if (data[i] != cnt++) {
        fprintf(stderr, "Reader %d saw incorrect data at %d vs %d\n", id, data[i], cnt);
        passed = false;
        break;
      }
    }

    remain -= nread;
  }
  stream.del_reader(id);
  return nbyte - remain;
}

static inline void stress_test(bool) {
  const int64_t kNbyte = int64_t{1} << 36; // 64 GB
  const int kNreader = 4;

  // Use a tiny buffer to force as much waiting as possible.
  zstream stream(SK_PAGE_SIZE);

  std::vector<std::future<ssize_t>> results;
  for (ssize_t ii = 0; ii < kNreader; ii++) {
    results.emplace_back(std::async(std::launch::async,
        reader_stress,
        stream.add_reader(),
        std::ref(stream),
        kNbyte));
  }

  std::future<ssize_t> resultw =
    std::async(std::launch::async, writer_stress, std::ref(stream), kNbyte);

  for (ssize_t ii = 0; ii < kNreader; ii++) {
    results[ii].wait();
  }
}

int main(int argc, char *argv[]) {
  CLI::App app{ "Simple FCZW" };

  bool borrow = false;
  bool stress = false;
  app.add_flag("-b,--borrow", borrow, "Use borrow API instead of read/write");
  app.add_flag("-s,--stress", stress, "Run stress test");

  CLI11_PARSE(app, argc, argv);
  if (stress) {
    stress_test(borrow);
  } else {
    benchmark(borrow);
  }
}
