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

        DEBUG(fprintf(stderr, "[w] writing %zd bytes total\n", nsamp_));
        ssize_t remain = nsamp_;
        while (remain) {
            ssize_t nwrite = std::min(remain, (ssize_t)data.size());
            DEBUG(fprintf(stderr, "[w] writing %zd bytes now\n", nwrite));
            remain -= stream_->write(data.data(), nwrite);
            DEBUG(fprintf(stderr, "\r[w]  %zd remain", remain));
        }
        DEBUG(fprintf(stderr, "\n[w] done\n"));

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
        DEBUG(fprintf(stderr, "[r] reading %zd bytes total\n", nsamp_));
        while (remain) {
            ssize_t size = std::min(remain, (ssize_t)data.size());
            DEBUG(fprintf(stderr, "[r] reading %zd bytes now\n", size));
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
    constexpr ssize_t buffer_sizes[] = {
        4096, 8192, 16384, 32768, 65536, 131072, 262144,
        524288, 1048576, 2097152, 4194304, 8388608
    };
    // constexpr ssize_t buffer_sizes[] = {
    //     65536
    // };

    constexpr ssize_t  num_readers[] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    //constexpr ssize_t  num_readers[] = { 1, 2 };
    //constexpr ssize_t  num_readers[] = { 4 };

    constexpr ssize_t nbyte = 1ull << 34; // 16GB

    printf("# bufsize  act_size  nreader  bytes/second-write  bytes/second-read  passed\n");
    for (ssize_t bufsize : buffer_sizes) {
        bool passed;
        for (ssize_t nreader : num_readers) {
            //for (double spin_time : spin_times) {
            zstream *stream = new zstream(bufsize);
            //stream->set_spin_limit(spin_time);

            //fprintf(stderr, "%7zd buffer, %zd readers\n", bufsize, nreader);

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
        //}
    }
}

int main(int argc, char* argv[]){
    CLI::App app{"Simple FCZW"};
    CLI11_PARSE(app, argc, argv);
    spdlog::info( "Initialized" );
    benchmark();
}
