#include <fzcw/sizeptr.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace ::testing;

TEST(sizeptr, ImplicitCastWorks) {
    struct A {};
    struct B : A {};

    sizeptr<B> b(new B, sizeof(B));
    sizeptr<A> a = b;
    EXPECT_THAT(a.get(),  Eq(b.get()));
    EXPECT_THAT(a.size(), Eq(b.size()));
}


TEST(sizeptr, AddingConstWorks) {
    struct A {};

    sizeptr<A> a(new A, sizeof(A));
    sizeptr<const A> b = a;

    EXPECT_THAT(a.get(),  Eq(b.get()));
    EXPECT_THAT(a.size(), Eq(b.size()));
}


TEST(sizeptr, ExplicitCastWorks) {
    struct A {};
    struct B {}; // No inheritance

    sizeptr<B> b(new B, sizeof(B));
    sizeptr<A> a = static_cast<sizeptr<A>>(b);
    EXPECT_THAT((void*)a.get(), Eq((void*)b.get()));
    EXPECT_THAT(a.size(), Eq(b.size()));
}


TEST(sizeptr, VoidPointersWork) {
    int val;
    sizeptr<void>       a(&val, sizeof(val));
    sizeptr<const void> b(&val, sizeof(val));

    EXPECT_THAT(a.get(),  Eq(b.get()));
    EXPECT_THAT(a.size(), Eq(b.size()));
}
