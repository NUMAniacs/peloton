# - Try to find jemalloc headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(Libnuma)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  NUMA_ROOT_DIR Set this variable to the root installation of
#                    libnuma if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  NUMA_FOUND             System has numa libs/headers
#  NUMA_LIBRARIES         The numa library/libraries
#  NUMA_INCLUDE_DIR       The location of numa headers
find_path(NUMA_ROOT_DIR
        NAMES include/numa.h
)

find_library(NUMA_LIBRARIES
        NAMES numa
        HINTS ${NUMA_ROOT_DIR}/lib
)

find_path(NUMA_INCLUDE_DIR
        NAMES numa.h
        HINTS ${NUMA_ROOT_DIR}/include
)


include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libnuma DEFAULT_MSG
        NUMA_LIBRARIES
        NUMA_INCLUDE_DIR
)

mark_as_advanced(
        NUMA_ROOT_DIR
        NUMA_LIBRARIES
        NUMA_INCLUDE_DIR
)

message(STATUS "Found Libnuma (include: ${NUMA_INCLUDE_DIRS}, library: ${NUMA_LIBRARIES})")