//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <mcor/mfile.h>

#include "context.h"
#include "database_imp.h"
#include "filenamer.h"
#include "garbage.h"
#include "trace.h"


// number of recently deleted files to be kept in a history,
// for debugging use only
static size_t deletedHistorySize = 100;

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

// uncomment to delete garbage in the context of the insert
//#define DELETE_IMMEDIATELY

namespace nabu
{

void
Garbage::Insert(const Entry& entry)
{
    DEBUG_LOCAL("Garbage::Insert %s\n", entry.Print().c_str());
#ifdef DELETE_IMMEDIATELY
    bool b = DeleteFile(entry);
    DEBUG_LOCAL("   %s\n", b ? "deleted" : "file not found");
#else
    cor::ObjectLocker ol(mMutex, "Insert");
    mCurrent.push_back(entry);
#endif
}

void
Garbage::Insert(const std::set<Entry>& entries)
{
    DEBUG_LOCAL("Garbage::Insert (set)%s\n", "");
#ifdef DELETE_IMMEDIATELY
    std::set<Entry>::const_iterator i = entries.begin();
    for (; i != entries.end(); i++)
    {
        DeleteFile(*i);
    }
    DEBUG_LOCAL("Files deleted%s\n","");
#else
    cor::ObjectLocker ol(mMutex, "InsertSet");
    std::set<Entry>::const_iterator i = entries.begin();
    for (; i != entries.end(); i++)
    {
        mCurrent.push_back(*i);
    }
    mCondition.Signal();
#endif
}

size_t
Garbage::Size() const
{
    size_t total = 0;
    cor::ObjectLocker ol(mMutex, "Size");
    return mCurrent.size();
}

bool
Garbage::IsEmpty() const
{
    cor::ObjectLocker ol(mMutex, "IsEmpty");
    return mCurrent.empty();
}

void
Garbage::WaitUntilEmpty()
{
    if (!IsRunning())
        throw cor::Exception("Garbage::WaitUntilEmpty() -- thread is not running");

    while (!IsEmpty())
        usleep(10000);
}

size_t
Garbage::EmptyAll()
{
    return Delete(0); // delete everything
}

void Garbage::EmptyAllInteractive()
{
    size_t total = Size();
    fprintf(stderr,"Emptying %ld files...\n", total);
    if (!IsRunning())
        Start();
    while (!IsEmpty())
    {
        size_t n = Size();
        fprintf(stderr, "\r%ld of %ld...", total - n, total);
        usleep(250000);
    }
    fprintf(stderr, "\r%ld of %ld...", total, total);
    printf("\n");

    Stop();
}

void
Garbage::Print(std::ostream& os)
{
    cor::ObjectLocker ol(mMutex, "Print");
    EntryList::const_iterator ci = mCurrent.begin();
    for (; ci != mCurrent.end(); ci++)
    {
        os << *ci << std::endl;
    }
}

void
Garbage::PrintHistory() const
{
    cor::ObjectLocker ol(mMutex, "PrintHistory");
    HistoryList::const_iterator i = mHistory.begin();
    for (; i != mHistory.end(); i++)
    {
        printf("   %s\n", i->c_str());
    }
}

void
Garbage::OnLock()
{
    //printf("Garbage collection disabled\n");
}

void
Garbage::OnUnlock()
{
    //printf("Garbage collection enabled\n");
}

Garbage::Garbage(DatabaseImp& database) :
    cor::Thread("Garbage"),
    cor::Lockout<std::string>("Garbage"),
    mDataBase(database),
    mMutex("Garbage", false),
    mCondition("Garbage", mMutex)
{
}

Garbage::~Garbage()
{
    if (IsRunning())
        Stop();
}

std::string
Garbage::Entry::Print() const
{
    std::string s = mTableAddress.GetLiteral();
    s += "(" + FileNamer::Extension(mType) + ")";
    return s;
}

void
Garbage::ThreadFunction()
{
    while (!StopNow())
    {
        try
        {
            if (IsLocked())
            {
                sleep(1);
            }
            else
            {
                Delete(10);
            }
        } catch (const cor::Exception& err)
        {
            printf("Error in garbage thread: %s\n", err.what());
            sleep(1);
        }
        //sleep(1);
    }
}

size_t
Garbage::Delete(size_t maxDelete)
{
    size_t count = 0; // number of files deleted

    cor::ObjectLocker ol(mMutex);

    if (!mCurrent.empty() || mCondition.Wait(1000))
    {
        // Don't hold mutex while calling out (back into mRoot) to avoid deadlock;
        // lists will not invalidate iterators during push_back, etc., so
        // the mutex can be released safely to allow incoming Insert()s which
        // will append to the back of the list as it is erased from the front
        // below
        EntryList::iterator fsi = mCurrent.begin();
        EntryList::iterator stop = mCurrent.end();
        ol.Unlock();

        while (fsi != stop)
        {
            // top-down delete of roots
            if (fsi->mType == FileType::eRoot)
            {
                Trace::Printf("Deleting root (%s)\n", fsi->mTableAddress.GetLiteral().c_str());
                size_t n = mDataBase.DeleteRoot(fsi->mTableAddress);
                Trace::Printf("Deleted %ld files from root '%s'\n", n, fsi->mTableAddress.GetLiteral().c_str());
                count += n;
            }

            // delete this file
            if (DeleteFile(*fsi))
                count++;

            {
                cor::ObjectLocker ol2(mMutex);
                fsi = mCurrent.erase(fsi);
            }

            if ((maxDelete != 0) && (count >= maxDelete))
            {
                return count; // enough for now
            }
       }
    }
    return count;
}

bool
Garbage::DeleteFile(const Entry &entry)
{
    // delete this file
    Trace::Printf("Checking %s (%s)\n", entry.mTableAddress.GetLiteral().c_str(), FileNamer::Extension(entry.mType).c_str());
    bool inUse = mDataBase.VersionInUse(entry.mTableAddress);

    Trace::Printf("%s is %s\n", entry.mTableAddress.GetLiteral().c_str(), inUse ? "in use" : "not in use");
    bool r = false;
    if (!inUse)
    {
        std::string s = mDataBase.GetFileNamer().NameOf(entry.mTableAddress, entry.mType);
        Trace::Printf("Garbage : deleting file '%s'\n", s.c_str());
        if (cor::File::Delete(s))
        {
            r = true;
            mHistory.push_back(s);
            if (mHistory.size() > deletedHistorySize)
                mHistory.pop_front();
        }
    }

    return r;
}

bool
operator>(const Garbage::Entry& e1, const Garbage::Entry& e2)
{
    return e1.mTableAddress > e2.mTableAddress;
}

bool
operator>=(const Garbage::Entry& e1, const Garbage::Entry& e2)
{
    return e1.mTableAddress >= e2.mTableAddress;
}

bool
operator<(const Garbage::Entry& e1, const Garbage::Entry& e2)
{
    return e1.mTableAddress < e2.mTableAddress;
}

bool
operator<=(const Garbage::Entry& e1, const Garbage::Entry& e2)
{
    return e1.mTableAddress <= e2.mTableAddress;
}

bool
operator==(const Garbage::Entry& e1, const Garbage::Entry& e2)
{
    return e1.mTableAddress == e2.mTableAddress;
}

bool
operator!=(const Garbage::Entry& e1, const Garbage::Entry& e2)
{
    return !(e1 == e2);
}

std::ostream&
operator<<(std::ostream& os, const Garbage::Entry& e)
{
    return os << e.mTableAddress << " (" << FileNamer::Extension(e.mType) << ")";
}

} //  end namespace
