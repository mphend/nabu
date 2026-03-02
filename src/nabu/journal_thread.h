//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_JOURNAL_THREAD_H
#define GICM_NABU_JOURNAL_THREAD_H

#include <map>
#include <set>
#include <string>

#include <mcor/mmutex.h>
#include <mcor/mthread.h>

#include "journal.h"

namespace nabu
{

class DatabaseImp;

/** class JournalThread : thread that flushes all journals from a Nabu root
 *
 */
class JournalThread : public cor::Thread
{
public:
    JournalThread(DatabaseImp& database);
    ~JournalThread();

private:
    DatabaseImp& mDataBase;

    void ThreadFunction();

};

} // end namespace

#endif
