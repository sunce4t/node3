# Author: Ming Zhang
# Copyright (c) 2022

set(SERVER_SOURCE server.cc)
add_executable(mem_pool ${SERVER_SOURCE})
target_link_libraries(mem_pool race_db rlib cityhash)
