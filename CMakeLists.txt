cmake_minimum_required(VERSION 3.4...3.12)
project(filecmd VERSION 0.0.0)

find_package(daq-buildtools 1.2.1 REQUIRED)
daq_setup_environment()

find_package(cmdlib REQUIRED)
find_package(folly REQUIRED)

set(FILECMD_DEPENDENCIES cmdlib::cmdlib)

# N.B. must start with lower case, do not "fix"
daq_add_plugin(fileCommandFacility duneCommandFacility LINK_LIBRARIES cmdlib)

daq_add_unit_test(FollyUri_test LINK_LIBRARIES Folly::folly)

daq_install()

