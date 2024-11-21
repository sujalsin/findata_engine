enable_testing()

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

add_executable(findata_tests
    main.cpp
    storage_engine_test.cpp
    memory_layer_test.cpp
    disk_layer_test.cpp
    benchmark.cpp
)

target_link_libraries(findata_tests
    PRIVATE
        findata_engine
        findata_engine_rs
        GTest::gtest
        GTest::gtest_main
        dl
        pthread
)

add_test(NAME findata_tests COMMAND findata_tests)
