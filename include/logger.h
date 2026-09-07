#pragma once
#include <fstream>
#include <mutex>

namespace ziplog
{
    inline std::ofstream &get_log()
    {
        static std::ofstream f("ziplog.log");
        return f;
    }
    inline std::mutex &get_log_mutex()
    {
        static std::mutex mu;
        return mu;
    }
}

#define ZIPLOG_VERBOSE 0

#if ZIPLOG_VERBOSE
#define ZLOG(x)                                                  \
    {                                                            \
        std::lock_guard<std::mutex> _l(ziplog::get_log_mutex()); \
        ziplog::get_log() << x << "\n";                          \
    }
#else
#define ZLOG(x)
#endif