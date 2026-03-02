//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_GARBAGE_H
#define GICM_NABU_GARBAGE_H

#include <list>
#include <string>


#include <mcor/lockout.h>
#include <mcor/mcondition.h>
#include <mcor/mmutex.h>
#include <mcor/mthread.h>

#include "private_types.h"
#include "table_address.h"

namespace nabu
{

class DatabaseImp;

/** class Garbage : Singleton threadsafe set of files to be deleted
 *
 */
class Garbage : public cor::Thread, public cor::Lockout<std::string>
{
public:
    Garbage(DatabaseImp& database);
    virtual ~Garbage();

    struct Entry {
        Entry() : mType(FileType::eData) {}
        Entry(const TableAddress& ga, FileType type ) : mTableAddress(ga), mType(type) {}

        TableAddress mTableAddress;
        FileType mType;

        std::string Print() const;
    };
    typedef std::map<TableAddress, Garbage::Entry> EntryMap;

    void Insert(const Entry& entry);
    void Insert(const std::set<Entry>& entries);

    size_t Size() const;
    bool IsEmpty() const;

    // waits until all garbage is emptied
    void WaitUntilEmpty();

    // synchronous call to empty garbage in the context of caller's thread;
    // this thread must not be running
    size_t EmptyAll();

    // EmptyAll() and print progress to os
    void EmptyAllInteractive();

    // for debug use only, this is not threadsafe
    void Print(std::ostream& os);

    void PrintHistory() const;

    // cor::Lockout
    void OnLock();
    void OnUnlock();

private:
    Garbage(const Garbage& other);

    DatabaseImp& mDataBase;

    typedef std::list<Entry> EntryList;

    // this mutex is not held by the background thread except to
    // access iterators. This is made safe by the public functions
    // not being able to do anything that invalidate those iterators.
    mutable cor::MMutex mMutex;
    cor::MCondition mCondition;
    EntryList mCurrent;

    void ThreadFunction();

    // returns the number of items actually deleted, up to maxDelete
    // if nonzero
    size_t Delete(size_t maxDelete);

    bool DeleteFile(const Entry& entry);

    // list of some recently deleted files, for debugging use only
    typedef std::list<std::string> HistoryList;
    HistoryList mHistory;
};

bool operator>(const Garbage::Entry& e1, const Garbage::Entry& e2);
bool operator>=(const Garbage::Entry& e1, const Garbage::Entry& e2);
bool operator<(const Garbage::Entry& e1, const Garbage::Entry& e2);
bool operator<=(const Garbage::Entry& e1, const Garbage::Entry& e2);
bool operator==(const Garbage::Entry& e1, const Garbage::Entry& e2);
bool operator!=(const Garbage::Entry& e1, const Garbage::Entry& e2);
std::ostream& operator<<(std::ostream& os, const Garbage::Entry& e);

} // end namespace

#endif
