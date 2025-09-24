cmake_minimum_required(VERSION 3.10)
project(data_analysis)
if(POLICY CMP0146)
    cmake_policy(SET CMP0146 NEW)
endif()
set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 在CMakeLists.txt中添加以下标志
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -matomics")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -matomics")

# 只编译静态库，防止 wasm 动态库报错
set(MZ_FETCH_LIBS OFF CACHE BOOL "" FORCE)
set(MZ_OPENSSL OFF CACHE BOOL "" FORCE)
set(MZ_ZLIB ON CACHE BOOL "" FORCE)
set(MZ_BZIP2 OFF CACHE BOOL "" FORCE)
set(MZ_LZMA OFF CACHE BOOL "" FORCE)
set(MZ_ZSTD OFF CACHE BOOL "" FORCE)
set(MZ_PKCS12 OFF CACHE BOOL "" FORCE)
set(MZ_COMPAT ON CACHE BOOL "" FORCE)
set(MZ_SIGNING OFF CACHE BOOL "" FORCE)
set(MZ_ICONV OFF CACHE BOOL "" FORCE)
set(MZ_LIBBSD OFF CACHE BOOL "" FORCE)
set(MZ_PKCS7 OFF CACHE BOOL "" FORCE)
set(MZ_WZAES OFF CACHE BOOL "" FORCE)
set(MZ_BUILD_TESTS OFF CACHE BOOL "" FORCE)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(STATIC ON CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ZLIB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZLIB_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(USE_SYSTEM_MINIZIP OFF CACHE BOOL "" FORCE)
set(USE_SYSTEM_ZLIB OFF CACHE BOOL "" FORCE)
set(ZLIB_ROOT "" CACHE STRING "" FORCE)
set(ZLIB_FOUND OFF CACHE BOOL "" FORCE)

set(EXPAT_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(EXPAT_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

# 添加 zlib
add_subdirectory(third_party/zlib)

# 添加 minizip（如果 libxlsxio 需要独立 minizip）
add_subdirectory(third_party/minizip)
# 假设 minizip 的 target 名为 minizip（如有不同请用实际 target 名）
set(MINIZIP_LIBRARIES minizip)
set(MINIZIP_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/third_party/minizip)
include_directories(${MINIZIP_INCLUDE_DIRS})

add_subdirectory(third_party/libexpat/expat) # expat 的 CMakeLists.txt 在 expat/expat 目录下
# 设置 EXPAT 库和头文件路径，供 xlsxio 查找
set(EXPAT_LIBRARY expat)
set(EXPAT_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/third_party/libexpat/expat/lib)

# 添加 libxlsxio
add_subdirectory(third_party/xlsxio)

add_subdirectory(third_party/xlnt)
set(XLNT_LIBRARIES xlnt)
set(XLNT_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/third_party/xlnt/include)
include_directories(${XLNT_INCLUDE_DIRS})

file(GLOB SOURCES src/*.cpp)
add_executable(${PROJECT_NAME} ${SOURCES})

if(EMSCRIPTEN)
    set(CMAKE_EXECUTABLE_SUFFIX ".js")
    set_target_properties(${PROJECT_NAME} PROPERTIES LINK_FLAGS
        "-sEXPORTED_FUNCTIONS=_main -sEXPORTED_RUNTIME_METHODS=ccall,cwrap,FS_writeFile -sUSE_ZLIB=1 -sALLOW_MEMORY_GROWTH"
    )
endif()

# 链接 libxlsxio_read、libxlsxio_write、zlib
target_link_libraries(${PROJECT_NAME}
    PRIVATE
        xlsxio_read_STATIC
        xlsxio_write_STATIC
        minizip
        zlibstatic
        expat
        xlnt
)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/xlsxio/include
    ${CMAKE_SOURCE_DIR}/third_party/minizip
    ${CMAKE_SOURCE_DIR}/third_party/libexpat/expat/lib
    ${CMAKE_SOURCE_DIR}/third_party/zlib
    include
)