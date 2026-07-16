find_package(Threads REQUIRED)

add_executable(podio_arrow_pipe_roundtrip podio_arrow_pipe_roundtrip.cpp)

target_link_libraries(podio_arrow_pipe_roundtrip PRIVATE
  podio::podio
  podio::podioRootIO
  podio::podioArrow
  ${PODIO_ARROW_TARGET}
  fmt::fmt
  Threads::Threads
  ${CMAKE_DL_LIBS}
)

if(TARGET TestDataModel)
  target_link_libraries(podio_arrow_pipe_roundtrip PRIVATE TestDataModel)
  target_compile_definitions(podio_arrow_pipe_roundtrip PRIVATE PODIO_ENABLE_TEST_DATAMODEL=1)
  # On Linux, Arrow registries for dynamic loading might require --no-as-needed
  if(TARGET TestDataModelArrow)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
      target_link_libraries(podio_arrow_pipe_roundtrip PRIVATE
        -Wl,--push-state,--no-as-needed
        TestDataModelArrow
        -Wl,--pop-state
      )
    else()
      target_link_libraries(podio_arrow_pipe_roundtrip PRIVATE TestDataModelArrow)
    endif()
  endif()
endif()

install(TARGETS podio_arrow_pipe_roundtrip EXPORT podioTargets DESTINATION ${CMAKE_INSTALL_BINDIR})
