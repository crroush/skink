#include <skink/random.h>
#include <skink/zstream.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <future>
#include <utility>
#include <vector>

using namespace ::sk;
using namespace ::testing;

template <typename T>
ssize_t writer(zstream &stream, ssize_t nbyte, int seed, bool randomize) {
  constexpr int64_t kBufSamp = 32768;
  constexpr int64_t kBufByte = kBufSamp * sizeof(T);

  // Truncate bytes to multiple of sample size.
  nbyte = nbyte / sizeof(T) * sizeof(T);

  xoros256ss rnd0(seed);
  xoros256ss rnd1(3);  // To generate random write sizes.

  std::vector<T> data(kBufSamp);
  int64_t remain_bytes = nbyte;
  while (remain_bytes > 0) {
    ssize_t nwrite_bytes = std::min(remain_bytes, kBufByte);
    if (randomize) {
      nwrite_bytes = std::min(
          remain_bytes,
          (int64_t)(rnd1.uniform_u64(kBufSamp / 4, kBufSamp) * sizeof(T)));
    }

    for (size_t ii = 0; ii < nwrite_bytes / sizeof(T); ++ii) {
      data[ii] = rnd0.uniform_int<T>();
    }

    ssize_t nwrote_bytes = stream.write(data.data(), nwrite_bytes);
    remain_bytes -= nwrote_bytes;
    if (nwrote_bytes < nwrite_bytes) {
      break;
    }
  }
  stream.wrclose();
  return nbyte - remain_bytes;
}

template <typename T>
bool reader(zstream &stream, int id, ssize_t nbyte, int seed, bool randomize) {
  constexpr int64_t kBufSamp = 32768;
  constexpr int64_t kBufByte = kBufSamp * sizeof(T);

  xoros256ss rnd0(seed);
  xoros256ss rnd1(5);  // To generate random read sizes.

  // Truncate bytes to multiple of sample size.
  nbyte = nbyte / sizeof(T) * sizeof(T);

  std::vector<T> data(32768);

  bool passed          = true;
  ssize_t remain_bytes = nbyte;
  while (remain_bytes) {
    ssize_t toread = std::min(remain_bytes, kBufByte);
    if (randomize) {
      toread = std::min(
          remain_bytes,
          (int64_t)(rnd1.uniform_u64(kBufSamp / 4, kBufSamp) * sizeof(T)));
    }

    ssize_t nread_bytes = stream.read(id, data.data(), toread, toread);
    if (nread_bytes < toread) {
      break;
    }
    remain_bytes -= nread_bytes;

    const int nsamp = nread_bytes / sizeof(T);
    for (ssize_t i = 0; i < nsamp; ++i) {
      passed &= (data[i] == rnd0.uniform_int<T>());
    }
  }

  stream.del_reader(id);
  printf( "%zd\n", remain_bytes);
  return passed;
}

template <typename T>
ssize_t borrow_writer(zstream &stream,
                      ssize_t nbyte,
                      int seed,
                      bool randomize) {
  constexpr int64_t kBufSamp = 32768;
  constexpr int64_t kBufByte = kBufSamp * sizeof(T);

  // Truncate bytes to multiple of sample size.
  nbyte = nbyte / sizeof(T) * sizeof(T);

  xoros256ss rnd0(seed);
  xoros256ss rnd1(50);  // To generate random write sizes.

  int64_t remain_bytes = nbyte;
  while (remain_bytes > 0) {
    ssize_t nwrite_bytes = std::min(remain_bytes, kBufByte);
    if (randomize) {
      nwrite_bytes = std::min(
          remain_bytes,
          (int64_t)(rnd1.uniform_u64(kBufSamp / 4, kBufSamp) * sizeof(T)));
    }

    T *data = static_cast<T *>(stream.wborrow(nwrite_bytes));
    if (data == nullptr) {
      break;
    }

    for (size_t ii = 0; ii < nwrite_bytes / sizeof(T); ++ii) {
      data[ii] = rnd0.uniform_int<T>();
    }
    stream.wrelease(nwrite_bytes);

    remain_bytes -= nwrite_bytes;
  }

  stream.wrclose();
  return nbyte - remain_bytes;
}

template <typename T>
bool borrow_reader(zstream &stream,
                   int id,
                   ssize_t nbyte,
                   int seed,
                   bool randomize) {
  constexpr int64_t kBufSamp = 32768;
  constexpr int64_t kBufByte = kBufSamp * sizeof(T);

  xoros256ss rnd0(seed);
  xoros256ss rnd1(id);  // To generate random read sizes.

  // Truncate bytes to multiple of sample size.
  nbyte = nbyte / sizeof(T) * sizeof(T);

  bool passed          = true;
  ssize_t remain_bytes = nbyte;
  while (remain_bytes) {
    ssize_t toread = std::min(remain_bytes, kBufByte);
    if (randomize) {
      toread = std::min(
          remain_bytes,
          (int64_t)(rnd1.uniform_u64(kBufSamp / 4, kBufSamp) * sizeof(T)));
    }

    sizeptr<const void> ptr = stream.rborrow(id, toread);
    if (!ptr) {
      printf( "Breaking!\n");
      break;
    }

    const T *data   = static_cast<const T *>(ptr.get());
    const int nsamp = ptr.size() / sizeof(T);
    for (ssize_t i = 0; i < nsamp; ++i) {
      passed &= (data[i] == rnd0.uniform_int<T>());
    }
    stream.rrelease(id, toread);

    remain_bytes -= ptr.size();
  }

  if ( remain_bytes != 0 ){
    printf( "This is an error %zd\n", remain_bytes);
    passed = false;
  }

  stream.del_reader(id);
  return remain_bytes == 0;
}

TEST(zstream, CreateWorks) {
  zstream stream;
  EXPECT_THAT(stream.size(), Eq(zstream::kDefaultSize));
}

TEST(zstream, ResizeWorks) {
  constexpr int kOneMb = 1024 * 1024;

  zstream stream;
  EXPECT_THAT(stream.size(), Eq(zstream::kDefaultSize));
  stream.resize(kOneMb);
  EXPECT_THAT(stream.size(), Ge(kOneMb));
}

template <typename T>
bool TestStream(int num_bytes, int num_reader, bool randomize) {
  zstream stream;

  std::vector<std::future<bool>> results;
  for (ssize_t ii = 0; ii < num_reader; ii++) {
    results.emplace_back(std::async(std::launch::async,
                                    reader<T>,
                                    std::ref(stream),
                                    stream.add_reader(),
                                    num_bytes,
                                    1,
                                    randomize));
  }

  // Create writers.
  std::future<ssize_t> resultw = std::async(std::launch::async,
                                            writer<T>,
                                            std::ref(stream),
                                            num_bytes,
                                            1,
                                            randomize);

  bool passed = true;
  for (ssize_t ii = 0; ii < num_reader; ii++) {
    passed &= results[ii].get();
  }

  EXPECT_THAT(resultw.get(), Eq(num_bytes));
  return passed;
}

//TEST(zstream, DataCorrectU8) {
//  EXPECT_THAT(TestStream<uint8_t>(1 << 26, 4, false), IsTrue());
//}
//
//TEST(zstream, DataCorrectI16) {
//  EXPECT_THAT(TestStream<int16_t>(1 << 26, 4, false), IsTrue());
//}
//
//TEST(zstream, DataCorrectU64) {
//  EXPECT_THAT(TestStream<uint64_t>(1 << 26, 4, false), IsTrue());
//}
//
//
//TEST(zstream, DataCorrectU8_Rand) {
//  EXPECT_THAT(TestStream<uint8_t>(1 << 26, 4, true), IsTrue());
//}
//
//TEST(zstream, DataCorrectI16_Rand) {
//  EXPECT_THAT(TestStream<int16_t>(1 << 26, 4, true), IsTrue());
//}
//
//TEST(zstream, DataCorrectU64_Rand) {
//  EXPECT_THAT(TestStream<uint64_t>(1 << 26, 4, true), IsTrue());
//}

template <typename T>
bool TestBorrowStream(int num_bytes, int num_reader, bool randomize) {
  zstream stream;

  std::vector<std::future<bool>> results;
  for (ssize_t ii = 0; ii < num_reader; ii++) {
    results.emplace_back(std::async(std::launch::async,
                                    borrow_reader<T>,
                                    std::ref(stream),
                                    stream.add_reader(),
                                    num_bytes,
                                    1,
                                    randomize));
  }

  // Create writers.
  std::future<ssize_t> resultw = std::async(std::launch::async,
                                            borrow_writer<T>,
                                            std::ref(stream),
                                            num_bytes,
                                            1,
                                            randomize);

  bool passed = true;
  for (ssize_t ii = 0; ii < num_reader; ii++) {
    passed &= results[ii].get();
  }
  EXPECT_THAT(resultw.get(), Eq(num_bytes));
  return passed;
}

//TEST(zstream, DataCorrectU8Borrow) {
//  EXPECT_THAT(TestBorrowStream<uint8_t>(1 << 26, 4, false), IsTrue());
//}
//
//TEST(zstream, DataCorrectI16Borrow) {
//  EXPECT_THAT(TestBorrowStream<int16_t>(1 << 26, 4, false), IsTrue());
//}
//
//TEST(zstream, DataCorrectU64Borrow) {
//  EXPECT_THAT(TestBorrowStream<uint64_t>(1 << 26, 4, false), IsTrue());
//}

//TEST(zstream, DataCorrectU8Borrow_Rand) {
//  EXPECT_THAT(TestBorrowStream<uint8_t>(16384*8, 1, true), IsTrue());
//}

//TEST(zstream, DataCorrectI16Borrow_Rand) {
//  EXPECT_THAT(TestBorrowStream<int16_t>(1 << 26, 4, true), IsTrue());
//}
//
TEST(zstream, DataCorrectU64Borrow_Rand) {
  EXPECT_THAT(TestBorrowStream<uint64_t>(1 << 26, 1, true), IsTrue());
}
