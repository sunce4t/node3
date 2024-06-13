file(REMOVE_RECURSE
  "librlib.a"
  "librlib.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/rlib.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
