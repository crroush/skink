#include <skink/flexptr.h>

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace ::testing;

using ::testing::AllOf;
using ::testing::Ge;
using ::testing::IsTrue;
using ::testing::Le;
using ::testing::Lt;

TEST(flexptr, BasicFunctionality) {
  // Should be able to create const and non-const variants.
  int dummy;
  flexptr<int> f0       = &dummy;
  flexptr<const int> f1 = std::make_shared<const int>(10);

  // And check them for null-ness.
  int cnt = 0;
  if (f0) ++cnt;
  if (f1) ++cnt;

  EXPECT_THAT(cnt, Eq(2));

  // And move them to a new variable.
  flexptr<int> f2 = std::move(f0);
  EXPECT_THAT(f2.get(), Eq(&dummy));
  EXPECT_THAT(static_cast<bool>(f2), IsTrue());

  // Const conversion should be ok.
  f1 = std::move(f2);
  EXPECT_THAT(f1.get(), Eq(&dummy));

  f1.clear();
  EXPECT_THAT(f1.get(), Eq(nullptr));
}
