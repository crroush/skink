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

static inline void benchmark() {
  constexpr ssize_t nbyte = 1ull << 34;  // 16GB

  printf("# benchmarking read/write API\n");
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
        results.emplace_back(std::async(std::launch::async,
                                        reader,
                                        stream.add_reader(),
                                        std::ref(stream),
                                        nbyte));
      }

      // Create writers.
      std::future<ssize_t> resultw =
          std::async(std::launch::async, writer, std::ref(stream), nbyte);

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

static inline void benchmark_borrow() {
  constexpr ssize_t nbyte = 1ull << 34;  // 16GB

  printf("# benchmarking borrow API\n");
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
        results.emplace_back(std::async(std::launch::async,
                                        reader_borrow,
                                        stream.add_reader(),
                                        std::ref(stream),
                                        nbyte));
      }

      // Create writers.
      std::future<ssize_t> resultw =
          std::async(std::launch::async, writer_borrow, std::ref(stream), nbyte);

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


int main(int argc, char *argv[]) {
  CLI::App app{ "Simple FCZW" };

  bool borrow = false;
  app.add_flag("-b,--borrow", borrow, "Use borrow API instead of read/write");

  CLI11_PARSE(app, argc, argv);
  if (borrow) {
    benchmark_borrow();
  } else {
    benchmark();
  }
}
