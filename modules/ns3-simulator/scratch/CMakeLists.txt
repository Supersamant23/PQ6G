# CMakeLists for PQG6 NS-3 scratch module
# NS-3.41 scratch programs use build_lib_example

# Find liboqs (installed to /usr/local)
find_library(OQS_LIBRARY NAMES oqs REQUIRED)

build_lib_example(
    NAME pqg6-sim
    SOURCE_FILES pqg6-sim.cc
    LIBRARIES_TO_LINK
        ${libcore}
        ${libnetwork}
        ${libinternet}
        ${libpoint-to-point}
        ${libapplications}
        ${libflow-monitor}
        ${libtraffic-control}
        ${OQS_LIBRARY}
)
