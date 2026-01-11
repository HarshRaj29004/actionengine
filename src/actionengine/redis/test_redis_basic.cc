// Copyright 2025 Google LLC
// Licensed under the Apache License, Version 2.0

#include <memory>
#include <string>

#include <absl/status/statusor.h>
#include <absl/strings/numbers.h>
#include <absl/strings/string_view.h>
#include <absl/time/time.h>
#include <gtest/gtest.h>

#include "actionengine/redis/redis.h"
#include "actionengine/redis/test_helpers.h"  // Factored common helpers

namespace act::redis {

namespace {

// Common TryConnect now lives in test_helpers.h

TEST(RedisBasicTest, ConnectAndHelloOrSkip) {
  const auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  auto hello = redis->Hello();
  ASSERT_TRUE(hello.ok()) << hello.status();
  // Version is a string; require it to be non-empty and numeric >= 2 if parsable.
  EXPECT_FALSE(hello->version.empty());
  int v = 0;
  if (absl::SimpleAtoi(hello->version, &v)) {
    EXPECT_GE(v, 2);
  }
}

TEST(RedisBasicTest, SetGetRoundtrip) {
  const auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  redis->SetKeyPrefix("unittest:");

  const std::string key = "redis_basic_roundtrip";
  const std::string value = "hello";

  const auto st = redis->Set(key, value);
  ASSERT_TRUE(st.ok()) << st;

  auto got = redis->Get<std::string>(key);
  ASSERT_TRUE(got.ok()) << got.status();
  EXPECT_EQ(*got, value);
}

}  // namespace

}  // namespace act::redis
