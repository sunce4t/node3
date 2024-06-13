# Author: Ming Zhang
# Copyright (c) 2022

set(RUN_SRC run.cc)
add_executable(hashrun ${RUN_SRC})
target_link_libraries(hashrun worker)
