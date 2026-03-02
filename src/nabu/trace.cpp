//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "trace.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu
{

static bool enabled = false;

void Trace::Enable(bool b) { enabled = b; }

void Trace::Printf(const char *format, ...)
{
    if (!enabled)
        return;

    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end (args);
}

} //  end namespace
