//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <list>
#include <map>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <mcor/mmutex.h>
#include <mcor/mthread.h>

#include "context.h"

namespace nabu
{

class ContextManager
{
public:
    ContextManager() : mMutex("ContextManager") {}
    ~ContextManager() {}

    void PushContext(pthread_t threadId, const std::string& s);
    void PopContext(pthread_t threadId);

    void Dump();
    void DumpThread(pthread_t threadId);

private:
    mutable cor::MMutex mMutex;
    typedef std::map<pthread_t, std::list<std::string> > Imp;
    Imp mImp;

} contextManager;

void
ContextManager::PushContext(pthread_t threadId, const std::string &s)
{
    cor::ObjectLocker ol(mMutex);
    mImp[threadId].push_back(s);
}

void
ContextManager::PopContext(pthread_t threadId)
{
    cor::ObjectLocker ol(mMutex);
    mImp[threadId].pop_back();
    if (mImp[threadId].empty())
        mImp.erase(threadId);
}

void
ContextManager::Dump()
{
    cor::ObjectLocker ol(mMutex);
    Imp::const_iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        DumpThread(i->first);
    }
}

void
ContextManager::DumpThread(pthread_t threadId)
{
    cor::ObjectLocker ol(mMutex);

    Imp::const_iterator i = mImp.find(threadId);
    if (i == mImp.end())
        throw cor::Exception("ContextManager::DumpThread -- can't find thread %d\n", (long)threadId);

    printf("Thread %ld:\n", threadId);
    std::list<std::string>::const_iterator j = i->second.begin();
    std::string indent = "   ";
    for (; j != i->second.end(); j++)
    {
        printf("%s%s\n", indent.c_str(), j->c_str());
        indent += "   ";
    }
}

void
DumpContext()
{
    contextManager.Dump();
}

void
DumpThisContext()
{
    contextManager.DumpThread(cor::Thread::Self());
}

Context::Context(const char *format, ...)
{
    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end (args);
    contextManager.PushContext(cor::Thread::Self(), buffer);
}

Context::~Context()
{
    contextManager.PopContext(cor::Thread::Self());
}

} //  end namespace
