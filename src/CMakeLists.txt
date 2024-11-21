add_library(findata_engine
    memory_layer.cpp
    disk_layer.cpp
    storage_engine.cpp
    utils.cpp
)

target_include_directories(findata_engine
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(findata_engine
    PUBLIC
        Boost::boost
        Threads::Threads
        findata_engine_rs
)
