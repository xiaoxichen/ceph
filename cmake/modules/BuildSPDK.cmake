macro(build_spdk)
  if(NOT TARGET dpdk-ext)
    include(BuildDPDK)
    build_dpdk()
  endif()
  find_package(CUnit REQUIRED)
  include(ExternalProject)
  ExternalProject_Add(spdk-ext
    DEPENDS dpdk-ext
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/spdk
    CONFIGURE_COMMAND ./configure --with-dpdk=${DPDK_DIR}
    # unset $CFLAGS, otherwise it will interfere with how SPDK sets
    # its include directory.
    # unset $LDFLAGS, otherwise SPDK will fail to mock some functions.
    BUILD_COMMAND env -i PATH=$ENV{PATH} CC=${CMAKE_C_COMPILER} $(MAKE) EXTRA_CFLAGS="-fPIC"
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND "true")
  ExternalProject_Get_Property(spdk-ext source_dir)
  foreach(c nvme log env_dpdk util)
    add_library(spdk::${c} STATIC IMPORTED)
    add_dependencies(spdk::${c} spdk-ext)
    set_target_properties(spdk::${c} PROPERTIES
      IMPORTED_LOCATION "${source_dir}/build/lib/${CMAKE_STATIC_LIBRARY_PREFIX}spdk_${c}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${source_dir}/include")
    list(APPEND SPDK_LIBRARIES spdk::${c})
  endforeach()
  list(APPEND iface_libs ${DPDK_LIBRARIES} rt)
  set_target_properties(spdk::env_dpdk PROPERTIES
    INTERFACE_LINK_LIBRARIES "${iface_libs}")
  set(SPDK_INCLUDE_DIR "${source_dir}/include")
  unset(source_dir)
  unset(iface_libs)
endmacro()
