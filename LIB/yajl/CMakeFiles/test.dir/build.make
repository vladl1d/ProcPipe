# CMAKE generated file: DO NOT EDIT!
# Generated by "NMake Makefiles" Generator, CMake Version 3.13

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

!IF "$(OS)" == "Windows_NT"
NULL=
!ELSE
NULL=nul
!ENDIF
SHELL = cmd.exe

# The CMake executable.
CMAKE_COMMAND = D:\Software\cmake\bin\cmake.exe

# The command to remove a file.
RM = D:\Software\cmake\bin\cmake.exe -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = D:\Projects\test\yajl

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = D:\Projects\test\yajl\build

# Utility rule file for test.

# Include the progress variables for this target.
include CMakeFiles\test.dir\progress.make

CMakeFiles\test:
	cd D:\Projects\test\yajl\test\parsing
	.\run_tests.sh D:/Projects/test/yajl/build/test/parsing/yajl_test
	cd D:\Projects\test\yajl\build

test: CMakeFiles\test
test: CMakeFiles\test.dir\build.make

.PHONY : test

# Rule to build all files generated by this target.
CMakeFiles\test.dir\build: test

.PHONY : CMakeFiles\test.dir\build

CMakeFiles\test.dir\clean:
	$(CMAKE_COMMAND) -P CMakeFiles\test.dir\cmake_clean.cmake
.PHONY : CMakeFiles\test.dir\clean

CMakeFiles\test.dir\depend:
	$(CMAKE_COMMAND) -E cmake_depends "NMake Makefiles" D:\Projects\test\yajl D:\Projects\test\yajl D:\Projects\test\yajl\build D:\Projects\test\yajl\build D:\Projects\test\yajl\build\CMakeFiles\test.dir\DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles\test.dir\depend

