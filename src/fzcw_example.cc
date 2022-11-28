#include <fzcw/fzcw.h>

#include <algorithm>
#include <future>
#include <vector>

#include <stdio.h>

#include <spdlog/spdlog.h>

#include <CLI/App.hpp>
#include <CLI/Formatter.hpp>
#include <CLI/Config.hpp>

// Return current wall clock time, in seconds.
inline double stopwatch() {
    struct timespec tv;
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return tv.tv_sec + (double)tv.tv_nsec/1e9;
}

// Return time elapsed since a given start time, in seconds.
inline double stopwatch(double start) {
    return stopwatch()-start;
}

struct TestWriter {
    TestWriter(zstream& stream, ssize_t nsamp)
        : stream_(&stream), nsamp_(nsamp) {}

    ssize_t operator()() {
        std::vector<char> data(32768);

        ssize_t remain = nsamp_;
        while (remain) {
            ssize_t nwrite = std::min(remain, (ssize_t)data.size());
            remain -= stream_->write(data.data(), nwrite);
        }

        return nsamp_ - remain;
    }

private:
    zstream* stream_;
    ssize_t nsamp_;
};

struct TestReader {
    TestReader(int id, zstream& stream, ssize_t nsamp)
        : id_(id), stream_(&stream), nsamp_(nsamp) {}

    ssize_t operator()() {
        std::vector<char> data(32768);

        ssize_t remain = nsamp_;
        while (remain) {
            ssize_t size = std::min(remain, (ssize_t)data.size());
            ssize_t nread = stream_->read(id_, data.data(), size, size);
            if (nread < 0) {
                break;
            }
            remain -= nread;
        }
        return nsamp_ - remain;
    }

private:
    int id_;
    zstream* stream_;
    ssize_t nsamp_;
};


static inline void benchmark() {
    constexpr ssize_t nbyte = 1ull << 34; // 16GB

    printf("# bufsize  act_size  nreader  bytes/second-write  bytes/second-read  passed\n");
    for (int i=12; i <= 23; ++i) {
        const ssize_t bufsize = 1ull << i;
        bool passed;
        for (ssize_t nreader = 1; nreader <= 8; ++nreader) {
            zstream *stream = new zstream(bufsize);

            // Create readers.
            double tstart = stopwatch();
            std::vector<std::future<ssize_t>> results;
            for (ssize_t ii=0; ii < nreader; ii++) {
                results.emplace_back( \
                    std::async(std::launch::async, TestReader(stream->add_reader(), *stream, nbyte)));
            }

            // Create writers.
            std::future<ssize_t> resultw = \
                std::async(std::launch::async, TestWriter(*stream, nbyte));

            passed = true;
            for (ssize_t ii=0; ii < nreader; ii++) {
                passed &= (results[ii].get() == nbyte);
            }

            double telapsed = stopwatch(tstart);
            printf("%8zd %8zd %zd %.9e %.9e %d\n", bufsize, stream->size(), nreader, nbyte/telapsed, nreader*nbyte/telapsed, passed);
            fflush(stdout);
        }
    }
}

int main(int argc, char* argv[]){
    CLI::App app{"Simple FCZW"};
    CLI11_PARSE(app, argc, argv);
    benchmark();
}
