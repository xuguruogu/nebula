
# The base_obj is a basic library. Other libraries are not allowed to depend on it.

add_library(
    base_obj OBJECT
    Base.cpp
    Cord.cpp
    Configuration.cpp
    Status.cpp
    SanitizerOptions.cpp
    SignalHandler.cpp
    SlowOpTracker.cpp
    FlattenList.cpp ../../graph/Whitelist.cpp ../../graph/Whitelist.h ../utils/to_string.h)

IF(${PCHSupport_FOUND})
    add_precompiled_header(base_obj Base.h)
ENDIF(${PCHSupport_FOUND})

add_custom_target(
    clean-pch
    COMMAND "rm" "-fr" "Base.h.gch"
)

nebula_add_subdirectory(test)


