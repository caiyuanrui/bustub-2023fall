#include "spdlog/spdlog.h"

// %l: log level
// %s: file name
// %#: line
// %!: func name
// %v message
#define INFO(format_str, ...)                            \
  do {                                                   \
    spdlog::set_pattern("[%l] [%s:%#] [%!] %v");         \
    SPDLOG_INFO(fmt::format(format_str, ##__VA_ARGS__)); \
  } while (0)
