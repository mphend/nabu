//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CONTEXT_H
#define GICM_NABU_CONTEXT_H



namespace nabu
{

class Context
{
public:
    Context(const char* format, ...);
    ~Context();
};

// everything
void DumpContext();

// just this thread
void DumpThisContext();

} // end namespace

#endif
