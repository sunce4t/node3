# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/dell/sqs/race_share

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/dell/sqs/race_share/build

# Include any dependencies generated for this target.
include thirdparty/rlib/CMakeFiles/rlib.dir/depend.make

# Include the progress variables for this target.
include thirdparty/rlib/CMakeFiles/rlib.dir/progress.make

# Include the compile flags for this target's objects.
include thirdparty/rlib/CMakeFiles/rlib.dir/flags.make

# Object files for target rlib
rlib_OBJECTS =

# External object files for target rlib
rlib_EXTERNAL_OBJECTS =

thirdparty/rlib/librlib.a: thirdparty/rlib/CMakeFiles/rlib.dir/build.make
thirdparty/rlib/librlib.a: thirdparty/rlib/CMakeFiles/rlib.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/dell/sqs/race_share/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Linking CXX static library librlib.a"
	cd /home/dell/sqs/race_share/build/thirdparty/rlib && $(CMAKE_COMMAND) -P CMakeFiles/rlib.dir/cmake_clean_target.cmake
	cd /home/dell/sqs/race_share/build/thirdparty/rlib && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/rlib.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
thirdparty/rlib/CMakeFiles/rlib.dir/build: thirdparty/rlib/librlib.a

.PHONY : thirdparty/rlib/CMakeFiles/rlib.dir/build

thirdparty/rlib/CMakeFiles/rlib.dir/clean:
	cd /home/dell/sqs/race_share/build/thirdparty/rlib && $(CMAKE_COMMAND) -P CMakeFiles/rlib.dir/cmake_clean.cmake
.PHONY : thirdparty/rlib/CMakeFiles/rlib.dir/clean

thirdparty/rlib/CMakeFiles/rlib.dir/depend:
	cd /home/dell/sqs/race_share/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/dell/sqs/race_share /home/dell/sqs/race_share/thirdparty/rlib /home/dell/sqs/race_share/build /home/dell/sqs/race_share/build/thirdparty/rlib /home/dell/sqs/race_share/build/thirdparty/rlib/CMakeFiles/rlib.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : thirdparty/rlib/CMakeFiles/rlib.dir/depend
