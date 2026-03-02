//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_TRACE_H
#define GICM_NABU_TRACE_H



namespace nabu
{

class Trace
{
public:
    static void Printf(const char* format, ... );

    static void Enable(bool b);
};

} // end namespace

#endif
