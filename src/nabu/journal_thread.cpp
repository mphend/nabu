//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/mfile.h>

#include "database_imp.h"
#include "journal_thread.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu
{

JournalThread::JournalThread(DatabaseImp& database) :
    cor::Thread("JournalThread"),
    mDataBase(database)
{}

JournalThread::~JournalThread()
{
    if (IsRunning())
    {
        Stop();
    }
}

void
JournalThread::ThreadFunction()
{
    while (!StopNow())
    {
        try {
            mDataBase.FlushJournals();
        } catch (const cor::Exception& err)
        {
            printf("Error in journal thread: %s\n", err.what());
        }
        sleep(1);
    }
}

} //  end namespace
