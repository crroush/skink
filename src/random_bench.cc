#include <skink/random.h>

#include <benchmark/benchmark.h>

using namespace ::sk;

static void BM_PRNGSpeed(benchmark::State &state) {
  prng rnd;
  for (auto _ : state) {
    benchmark::DoNotOptimize(rnd());
  }
}
BENCHMARK(BM_PRNGSpeed);
