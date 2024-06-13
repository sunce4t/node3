
set(DB_SOURCES race_db.cc)

add_library(race_db STATIC ${DB_SOURCES})

set_target_properties(race_db PROPERTIES LINKER_LANGUAGE CXX)
