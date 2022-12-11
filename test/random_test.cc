#include <skink/random.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace ::testing;

using ::testing::Ge;
using ::testing::Le;
using ::testing::Lt;
using ::testing::AllOf;

template <typename T>
void TestIntInRange(xoros256ss& prng) {
    EXPECT_THAT(prng.uniform_int<T>(),
        AllOf(
            Ge(std::numeric_limits<T>::min()),
            Le(std::numeric_limits<T>::max())));
}

TEST(random, UniformInRange) {
    xoros256ss prng(0);
    for (int i=0; i < 100; ++i) {
        EXPECT_THAT(prng.uniform_flt(), AllOf(Ge(0), Lt(1)));
        EXPECT_THAT(prng.uniform_dbl(), AllOf(Ge(0), Lt(1)));

        TestIntInRange<uint64_t>(prng);
        TestIntInRange<uint32_t>(prng);
        TestIntInRange<uint16_t>(prng);
        TestIntInRange<uint8_t>(prng);

        TestIntInRange<int64_t>(prng);
        TestIntInRange<int32_t>(prng);
        TestIntInRange<int16_t>(prng);
        TestIntInRange<int8_t>(prng);
    }
}
