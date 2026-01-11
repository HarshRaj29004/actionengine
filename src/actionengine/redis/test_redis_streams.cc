// Copyright 2025 Google LLC
// Licensed under the Apache License, Version 2.0

#include <memory>
#include <string>
#include <vector>

#include <absl/time/time.h>
#include <gtest/gtest.h>

#include "actionengine/redis/streams.h"
#include "actionengine/redis/test_helpers.h"  // Factored common helpers

namespace act::redis {
namespace {

TEST(RedisStreamsTest, XAddXReadXRangeBasic) {
  auto redis = test::TryConnect();
  if (!redis) {
    GTEST_SKIP() << "Redis server not available on 127.0.0.1:6379";
  }

  const std::string key = redis->GetKey("streams:test");
  RedisStream stream(redis.get(), key);

  // Add a message
  auto id_or = stream.XAdd({{"field", "value"}});
  ASSERT_TRUE(id_or.ok()) << id_or.status();
  StreamMessageId id = *id_or;

  // Read from 0 should include at least this message.
  auto msgs_or = stream.XRead(StreamMessageId{});
  ASSERT_TRUE(msgs_or.ok()) << msgs_or.status();
  ASSERT_FALSE(msgs_or->empty());

  // XRANGE should be able to fetch the exact message by id range
  auto range_or = stream.XRange(id, id);
  ASSERT_TRUE(range_or.ok()) << range_or.status();
  ASSERT_EQ(range_or->size(), 1u);
  EXPECT_EQ(range_or->at(0).fields.at("field"), std::string("value"));
}

}  // namespace
}  // namespace act::redis
