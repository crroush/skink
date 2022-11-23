#include <fzcw/fzcw.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace ::testing;

TEST(zstream, CreateWorks) {
    zstream stream;
    EXPECT_THAT(stream.size(), Eq(zstream::kDefaultSize));
}

TEST(zstream, ResizeWorks) {
    constexpr int kOneMb = 1024*1024;

    zstream stream;
    EXPECT_THAT(stream.size(), Eq(zstream::kDefaultSize));
    stream.resize(kOneMb);
    EXPECT_THAT(stream.size(), Ge(kOneMb));
}
