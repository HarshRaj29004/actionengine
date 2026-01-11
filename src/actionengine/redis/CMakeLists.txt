add_library(actRedis STATIC
        chunk_store.cc
        chunk_store.h
        pubsub.cc
        pubsub.h
        test_helpers.h
        redis.cc
        redis.h
        reply.cc
        reply.h
        reply_converters.cc
        reply_converters.h
        reply_parsers.cc
        reply_parsers.h
        streams.cc
        streams.h
)
add_library(act::redis ALIAS actRedis)
target_include_directories(
        actRedis PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actRedis PUBLIC
        Thread
)
target_link_libraries(
        actRedis PRIVATE
        cppack
        hiredis::hiredis
        act::concurrency
        act::data
        act::msgpack
        act::stores
        act::util
        libuv::uv_a
        uvw::uvw
)

# ------------------------------------------------------------------

add_library(
        actRedisPybind11 STATIC
)
add_library(
        act::redis::pybind11 ALIAS
        actRedisPybind11
)
target_sources(
        actRedisPybind11 PRIVATE
        chunk_store_pybind11.cc
)
target_sources(
        actRedisPybind11 PUBLIC
        chunk_store_pybind11.h
)
target_link_libraries(
        actRedisPybind11 PRIVATE
        act::concurrency
        act::redis
        cppack
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actRedisPybind11 PUBLIC
        ${ACTIONENGINE_INCLUDE_DIRS}
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)

install(
        TARGETS actRedis
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)

# ------------------------------------------------------------------

add_executable(
        test_redis_basic
        test_redis_basic.cc
)
target_include_directories(
        test_redis_basic PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        test_redis_basic
        absl::status_matchers
        act::redis
        GTest::gtest
        GTest::gtest_main
)
gtest_discover_tests(test_redis_basic DISCOVERY_TIMEOUT 60)

add_executable(
        test_redis_scripts
        test_redis_scripts.cc
)
target_include_directories(
        test_redis_scripts PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        test_redis_scripts
        absl::status_matchers
        act::redis
        GTest::gtest
        GTest::gtest_main
)
gtest_discover_tests(test_redis_scripts DISCOVERY_TIMEOUT 60)

add_executable(
        test_redis_zrange
        test_redis_zrange.cc
)
target_include_directories(
        test_redis_zrange PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        test_redis_zrange
        absl::status_matchers
        act::redis
        GTest::gtest
        GTest::gtest_main
)
gtest_discover_tests(test_redis_zrange DISCOVERY_TIMEOUT 60)

add_executable(
        test_redis_streams
        test_redis_streams.cc
)
target_include_directories(
        test_redis_streams PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
        ${Boost_INCLUDE_DIRS}
)
target_link_libraries(
        test_redis_streams
        absl::status_matchers
        act::redis
        GTest::gtest
        GTest::gtest_main
)
gtest_discover_tests(test_redis_streams DISCOVERY_TIMEOUT 60)

add_executable(
        test_redis_pubsub
        test_redis_pubsub.cc
)
target_include_directories(
        test_redis_pubsub PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
        ${Boost_INCLUDE_DIRS}
)
target_link_libraries(
        test_redis_pubsub
        absl::status_matchers
        act::redis
        GTest::gtest
        GTest::gtest_main
)
gtest_discover_tests(test_redis_pubsub DISCOVERY_TIMEOUT 60)
