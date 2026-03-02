//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "branch.h"
#include "cache.h"
#include "context.h"
#include "database_imp.h"
#include "exception.h"
#include "io_op_imp.h"
#include "leak_detector.h"
#include "profiler.h"
#include "trace.h"

#include <mcor/profiler.h>

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {



/** class IOOperationImp implementation
 *
 */

IOOperationImp::IOOperationImp(DatabaseImp& database,
                               const cor::TimeRange& timeRange,
                               IterationDirection direction,
                               const muuid::uuid& handle,
                               AccessImp* accessImp,
                               Nesting nesting) :
                               IOOperation(timeRange, direction, handle),
                               mDataBase(database),
                               mAccessImp(accessImp),
                               mAddedToAccess(false),
                               mNesting(nesting),
                               mReadImpMutex("ReadImpMap"),
                               mWriteImpMutex("WriteImpMap")
{
    // enforce that access must be open before operations can
    // reference it
    if (mNesting != eNested)
    {
        if (!mAccessImp->Open(0))
            throw cor::Exception("IOOperation -- access is not open");

        // this verifies the timerange is within that of the access
        mAccessImp->AddOperation(this);
        mAddedToAccess = true;
    }
    LeakDetector::Get().Increment("IOOperationImp");
}

IOOperationImp::~IOOperationImp()
{
    //printf("IOOperationImp::~IOOperationImp %s\n", mHandle.string().c_str());
    LeakDetector::Get().Decrement("IOOperationImp");
    Abort(); // discard anything not fully committed; normally empty

    if (mAddedToAccess)
    {
        try {
            mAccessImp->RemoveOperation(this);
        } catch (const cor::Exception& err)
        {
            printf("~IOOperation RemoveOperation() -- %s\n", err.what());
        }
    }
}

IOOperationImp::ReadImp::ReadImp() :
        mMutex("ReadImp"),
        mReadPathValid(false),
        mLastTimeRead(cor::Time::eInvalid),
        mReadIndex(0),
        mLastRegion(NULL)
{
    LeakDetector::Get().Increment("IOOperationImp::ReadImp");
}

IOOperationImp::ReadImp::~ReadImp()
{
    LeakDetector::Get().Decrement("IOOperationImp::ReadImp");
}

void
IOOperationImp::Read(const MetaKey& column,
                       RecordVector& records,
                       size_t countIn,
                       cor::Time until)
{
    records.clear();

    // default argument is positive inf (read until no more data is left) for forward
    // iteration. Rather than invent a "directionless infinite" type, translate +inf
    // to -inf for backwards iteration
    if ((mDirection == eIterateBackwards) && (until.IsInfinite()))
        until = cor::Time::NegativeInf();

    size_t count = countIn;

    cor::ObjectLocker ol(mReadImpMutex);
    ReadImp* imp = &mReadImpMap[column];
    cor::ObjectLocker iol(imp->mMutex);
    ol.Unlock();

    // see if there is any such column
    if (!GetBranchImp()->HasColumn(column))
        return;

    //const ColumnInfo& ci = mDataBase.GetColumnInfo(column);

    //if (ci.mRecordType == eTRR)
    {
        Record r;
        // find any TRR that starts before this range but still overlaps
        // (first time only)
        // FetchLastRecord is iteration-direction aware, so 'last' is really
        // 'next' for a backwards-iterating IOP
        if (imp->mReadBuffer.empty() &&
            FetchLastRecord(column, mTimeRange.First(), r, eExclusive))
        {
            if (mDirection == eIterateForwards)
            {
                if ((r.mTimeRange.First() < mTimeRange.First()) &&
                    (r.mTimeRange.Intersects(mTimeRange)))
                {
                    records.push_back(r);

                    if (count != 0)
                    {
                        if (count == 1)
                            return; // done
                        count--;
                    }
                    if (r.mTimeRange.Final() >= until)
                        return; // done
                }
            }
            else // backwards iteration
            {
                if ((r.mTimeRange.Final() > mTimeRange.Final()) &&
                    (r.mTimeRange.Intersects(mTimeRange)))
                {
                    records.push_back(r);

                    if (count != 0)
                    {
                        if (count == 1)
                            return; // done
                        count--;
                    }
                    if (r.mTimeRange.First() < until)
                        return; // done
                }
            }
        }
        ReadImpl(column, records, count, until);
    }
    //else
    //{
    //    ReadImpl(column, records, count, until);
    //}

}

void
IOOperationImp::ReadImpl(const MetaKey& column,
                       RecordVector& records,
                       size_t count,
                       cor::Time until)
{
    DEBUG_LOCAL("IOOperationImp::ReadImpl() -- reading up to %ld from '%s' until %s\n",
           count,
           column.GetLiteral().c_str(),
           until.Print().c_str());

    // imp mutex must be held during this function

    ReadImp* imp = &mReadImpMap[column];

    // Initialize path to point to beginning (or end, if iterating backwards) of time range,
    // first time only
    if (!imp->mReadPathValid)
    {
        MetaDataTable root = GetRoot();
        TimePath begin;
        if (!root->RootFind(mDataBase, begin, column, mDirection,
            mDirection == eIterateForwards ? mTimeRange.First() : mTimeRange.Final()))
        {
            if (mDirection == eIterateForwards)
            {
                DEBUG_LOCAL("IOOperationImp::ReadImpl() -- Could find no data at or before %s\n",
                       mTimeRange.First().Print().c_str());
            }
            else
            {
                DEBUG_LOCAL("IOOperationImp::ReadImpl() -- Could find no data at or after %s\n",
                       mTimeRange.Final().Print().c_str());
            }
            // note we leave mReadPathValid == false here deliberately; the path
            // is not valid, because nothing was there
            return;
        }
        // if this is past the access range, it is not readable for this IOP
        // XXX exclusive
        if (IsForward())
        {
            if (mDataBase.GetTimeScheme().GetTime(begin).First() > mTimeRange.Final())
            {
                DEBUG_LOCAL("IOOperationImp::ReadImpl() -- First data at %s is after IOP, beginning at %s\n",
                       mDataBase.GetTimeScheme().GetTime(begin).First().Print().c_str(),
                       mTimeRange.Final().Print().c_str());
                return;
            }
        }
        else
        {
            if (mDataBase.GetTimeScheme().GetTime(begin).Final() <= mTimeRange.First())
            {
                DEBUG_LOCAL("IOOperationImp::ReadImpl() -- First data at %s is before backwards-iterating IOP, beginning at %s\n",
                            mDataBase.GetTimeScheme().GetTime(begin).First().Print().c_str(),
                            mTimeRange.First().Print().c_str());
                return;
            }
        }
        imp->mReadTimePath = begin;
        imp->mReadPathValid = true;
    }

    size_t n = 0;
    while ((count == 0) || n < count)
    {
        Record r;
        if (imp->DataRemaining() == 0)
        {
            if (!FetchFromFile(column, imp))
            {
                if (IsBackward() || !FetchFromJournal(column, imp))
                {
                    break; // no more data
                }
            }
        }

        // read next record, but only advance index if we intend to keep it
        r = imp->mReadBuffer[imp->mReadIndex];
        if (IsForward())
        {
            if ((r.GetTime() > until) || (r.GetTime() > mTimeRange.Final()))
                break; // past the 'until' marker or end of IO range; the read is done
        }
        else // backwards
        {
            //printf("ReadImpl -- final time record read is at %s\n", r.GetTime().Print().c_str());
            //printf("mTimeRange = %s, until = %s\n", mTimeRange.Print().c_str(), until.Print().c_str());
            if ((r.GetTime() < until) || (r.GetTime() < mTimeRange.First()))
                break; // past the 'until' marker or end of IO range; the read is done
        }
        imp->mReadIndex++;

        // if record is before the time range for the read, skip it
        if (IsForward())
        {
            if (mTimeRange.First() > r.GetTime())
                continue;
        }
        else // backwards
        {
            if (mTimeRange.Final() < r.GetTime())
                continue;
        }

        records.push_back(r);

        n++;
    }
    DEBUG_LOCAL("IOOperationImp::ReadImpl() -- Read %ld records\n", records.size());
}

bool
IOOperationImp::ReadSeek(const MetaKey& column,
                         const cor::Time& whence)
{
    DEBUG_LOCAL("IOOperationImp::ReadSeek() -- seeking %s to %s%s\n",
                column.GetLiteral().c_str(),
                whence.Print().c_str(),
                mDirection == eIterateBackwards ? " (backwards)" : "");

    if (!mTimeRange.In(whence))
    {
        throw cor::Exception("IOOperationImp : ReadSeek attempted to seek out of range");
    }

    cor::ObjectLocker ol(mReadImpMutex);
    ReadImp* imp = &mReadImpMap[column];
    cor::ObjectLocker iol(imp->mMutex);
    ol.Unlock();

    imp->mReadBuffer.clear();
    imp->mReadIndex = 0;
    imp->mReadTimePath = mDataBase.GetTimeScheme().GetPath(whence);

    MetaDataTable root = GetRoot();
    Version version = root->GetCurrentVersion(mDataBase, column, imp->mReadTimePath);
    bool b = false;
    if (version.mNumber == Version().mNumber)
    {
        // no data at whence; look for next data
        if (root->RootAdvance(mDataBase, imp->mReadTimePath, column, mDirection))
        {
            b = FetchFromFile(column, imp);
        }
        else
        {
            // out of data, will return false below, and should clear the
            // read path to indicate no more data can be read subsequently
            imp->mReadTimePath.clear();
        }
    }
    else
    {
        b = FetchFromFile(column, imp);
    }

    // depending on direction, either drop all data up to or after whence
    if (b)
    {
        if (mDirection == eIterateForwards)
        {
            size_t i = 0;
            for (; i < imp->mReadBuffer.size(); i++)
            {
                const Record& r = imp->mReadBuffer[i];
                if (r.GetTime() < whence)
                {
                    imp->mReadIndex++;
                }
                else
                    break;
            }

            b = (i != imp->mReadBuffer.size());
        }
        else
        {
            size_t i = 0;
            for (; i < imp->mReadBuffer.size(); i++)
            {
                const Record& r = imp->mReadBuffer[i];
                if (r.GetTime() > whence)
                {
                    imp->mReadIndex++;
                }
                else
                    break;
            }

            b = (i != imp->mReadBuffer.size());
        }
    }

    if (b)
        imp->mReadPathValid = true;

    return b;
}

std::string
IOOperationImp::Print() const
{
    std::string s = mHandle.string() + "  " + mTimeRange.Print();
    if (IsForward())
        s += "\n";
    else
        s += " (backwards)\n";

    {
        cor::ObjectLocker ol(mReadImpMutex);
        if (mReadImpMap.empty())
            s += "  no reads\n";
        else
            s += "reads:\n";
        std::map<MetaKey, ReadImp>::const_iterator i = mReadImpMap.begin();
        for (; i != mReadImpMap.end(); i++)
        {
            s += "   " + i->first.GetLiteral() + "\n";
        }
    }

    {
        cor::ObjectLocker ol(mWriteImpMutex);
        if (mWriteImpMap.empty())
            s += "  no writes\n";
        else
            s += "writes:\n";
        std::map<MetaKey, WriteImp>::const_iterator i = mWriteImpMap.begin();
        for (; i != mWriteImpMap.end(); i++)
        {
            s += "   " + i->first.GetLiteral() + "\n";
        }
    }

    return s;
}

bool
IOOperationImp::FetchFromFile(const MetaKey& column, ReadImp* imp)
{
    DEBUG_LOCAL("IOOperationImp::FetchFromFile() -- reading '%s'\n", column.GetLiteral().c_str());
    Context ctx("FetchFromFile %s", column.GetLiteral().c_str());
    // end of data
    if (imp->mReadTimePath.empty())
    {
        return false;
    }

    const TimePath fetchTimePath = imp->mReadTimePath;
    const cor::TimeRange fetchTr = mDataBase.GetTimeScheme().GetTime(fetchTimePath);
    //printf("FetchFromFile: %s fetchTimePath = %s\n", column.GetLiteral().c_str(), fetchTr.Print().c_str());

    // do not read anything at all if this is already out of range; this will be
    // the case when file boundaries fall on top of range boundaries
    if (IsForward() && (fetchTr.First() > mTimeRange.Final()))
    {
        return false;
    }
    if (IsBackward() && (fetchTr.Final() < mTimeRange.First()))
    {
        return false;
    }

    //ColumnInfo columnInfo = mDataBase.GetColumnInfo(column);

    MetaDataTable root = GetRoot();

    Version version = root->GetCurrentVersion(mDataBase, column, fetchTimePath);

    // DEFENSIVE
    if (version.Invalid())
    {
        throw FatalException(GetBranchImp()->GetLiteral(),
                             "IOOperationImp::FetchFromFile() -- %s tried to read non-existent data at %s:%s",
                             mHandle.string().c_str(),
                             column.GetLiteral().c_str(),
                             fetchTimePath.GetLiteral().c_str());
    }

    std::string fileName = mDataBase.GetFileNamer().NameOf(column, fetchTimePath, version, FileType::eData);

    DEBUG_LOCAL("IOOperationImp::FetchFromFile() -- reading from file '%s'\n", fileName.c_str());


    // DEFENSIVE
    if (imp->DataRemaining())
    {
        throw FatalException(GetBranchImp()->GetLiteral(),
                "Read buffer is not empty (%ld of %ld) during FetchFromFile",
                imp->mReadIndex,
                imp->mReadBuffer.size());
    }
    imp->mReadBuffer.clear();
    imp->mReadIndex = 0;
    DataFile dataFile(fileName);
    dataFile.Read(imp->mReadBuffer, mDirection);
    if (!imp->mReadBuffer.empty())
    {
        imp->mLastTimeRead = imp->mReadBuffer.back().GetTime();
    }
    else
    {
        // SANITY
        throw cor::Exception("IOperationImp::FetchFromFile() -- read empty data file '%s'\n", fileName.c_str());
    }

    //printf("\n\nCalling Next...\n");
    root->RootAdvance(mDataBase, imp->mReadTimePath, column, mDirection);

    //printf("Timepath after advancing: %s\n", imp->mReadTimePath.GetLiteral().c_str());

    return true;
}

void
IOOperationImp::FetchAllInFile(const nabu::MetaKey &column,
                               const TimePath& timePath,
                               RecordVector& records)
{
    DEBUG_LOCAL("IOOperationImp::FetchAllInFile() -- '%s' %s\n",
                column.GetLiteral().c_str(),
                timePath.GetLiteral().c_str());

    records.clear();

    MetaDataTable root = GetRoot();
    Version version = root->GetCurrentVersion(mDataBase, column, timePath);
    if (!version.Valid())
    {
        //printf("No data file for %s:%s\n", column.GetLiteral().c_str(), timePath.GetLiteral().c_str());
        return; // no data file
    }

    std::string fileName = mDataBase.GetFileNamer().NameOf(column, timePath, version, FileType::eData);
    //printf("Reading all data from file %s\n", fileName.c_str());
    DataFile dataFile(fileName);
    dataFile.Read(records, mDirection);
}

bool
IOOperationImp::FetchLastRecord(const nabu::MetaKey& column,
                                    const cor::Time& mark,
                                    nabu::Record& record,
                                    Inclusivity inclusivity)
{
    DEBUG_LOCAL("IOOperationImp::FetchLastRecord -- column %s, before or at %s\n",
                column.GetLiteral().c_str(),
                mark.Print().c_str());
    MetaDataTable root = GetRoot();

    TimePath timePath;
    if (!root->RootFind(mDataBase, timePath, column, OppositeIteration(), mark))
    {
        //printf("No TRRs in %s at %s\n", column.GetLiteral().c_str(), mark.Print().c_str());
        return false; // no TRRs within this time range
    }

    // if exclusive, then if this is not 'before' then advance (backwards) one
    if (inclusivity == eExclusive)
    {
        if (mDataBase.GetTimeScheme().GetTime(timePath).First() == mark)
        {
            if (!root->RootAdvance(mDataBase, timePath, column, OppositeIteration()))
                return false;
        }
    }

    // timePath points to final block of records in this time range
    RecordVector records;
    FetchAllInFile(column, timePath, records);

    // SANITY
    if (records.empty())
    {
        throw FatalException(GetBranchImp()->GetLiteral(),
                             "IOOperationImp::FetchLastRecord -- records are empty at %s:%s",
                             column.GetLiteral().c_str(),
                             timePath.GetLiteral().c_str());
    }

    // find last record that is < or <= (depending on Inclusivity) the mark
    //size_t i = 0;

    if (IsForward())
    {
        nabu::RecordVector::iterator i = records.begin();

        if (inclusivity == eInclusive)
        {
            //for (; i < records.size(); i++)
            for (; i != records.end(); i++)
            {
                if (i->mTimeRange.First() > mark)
                    break;
            }
        }
        else
        {
            //for (; i < records.size(); i++)
            for (; i != records.end(); i++)
            {
                if (i->mTimeRange.First() >= mark)
                    break;
            }
        }

        // i can be begin() in the case of an inclusive match; otherwise, it is the
        // element after the first invalid record. It can't be begin() for an exclusive
        // match due to the Find stuff above.
        if (i != records.begin())
            i--;

        record = *i;
    }
    else // backwards
    {
        nabu::RecordVector::reverse_iterator i = records.rbegin();

        if (inclusivity == eInclusive)
        {
            //for (; i < records.size(); i++)
            for (; i != records.rend(); i++)
            {
                if (i->mTimeRange.First() > mark)
                    break;
            }
        }
        else
        {
            //for (; i < records.size(); i++)
            for (; i != records.rend(); i++)
            {
                if (i->mTimeRange.First() >= mark)
                    break;
            }
        }

        // i can be rbegin() in the case of an inclusive match; otherwise, it is the
        // element after the first invalid record. It can't be rbegin() for an exclusive
        // match due to the Find stuff above.
        if (i != records.rbegin())
            i--;

        record = *i;
    }


    return true;
}

bool
IOOperationImp::FetchFromJournal(const MetaKey& column, ReadImp* imp)
{
    DEBUG_LOCAL("IOOperationImp::FetchFromJournal() -- reading '%s'\n", column.GetLiteral().c_str());

    Journal j = GetBranchImp()->GetJournal(column);
    if (!j)
        return false;

    cor::Time readStart = imp->mLastTimeRead;
    if (!readStart.Valid())
        readStart = mTimeRange.First();

    if (readStart == mTimeRange.Final())
        return false; // done

    if (imp->DataRemaining())
    {
        throw FatalException(GetBranchImp()->GetLiteral(),
                             "Read buffer is not empty (%ld of %ld) during FetchFromFile",
                             imp->mReadIndex,
                             imp->mReadBuffer.size());
    }
    imp->mReadBuffer.clear();
    imp->mReadIndex = 0;


    DEBUG_LOCAL("Reading from journal %ld to %ld\n",
                readStart.SecondPortion(),
                mTimeRange.Final().SecondPortion());
    j->Read(cor::TimeRange(readStart, mTimeRange.Final()), imp->mReadBuffer);

    if (!imp->mReadBuffer.empty())
    {
        DEBUG_LOCAL("Fetched %ld to %ld from journal\n",
                    imp->mReadBuffer.front().GetTime().SecondPortion(),
                    imp->mReadBuffer.back().GetTime().SecondPortion());
        imp->mLastTimeRead = imp->mReadBuffer.back().GetTime();
        return true;
    }

    return false;
}

MetaDataTable
IOOperationImp::GetRoot()
{
    // if working from a tag, use that root; otherwise, use head
    if (!GetTagName().empty())
    {
        return GetBranchImp()->GetTagMetaData(GetTagName());
    }
    else
    {
        return GetBranchImp()->GetHead();
    }
}

    
}
