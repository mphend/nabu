//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <sstream>
#include <stdio.h>

#include <mcor/mthread.h>
#include <mcor/thread_set.h>
#include <mcor/json_time.h>

#include "io_op.h"
#include "leak_detector.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

std::string
SearchResult::Print() const
{
    std::ostringstream oss;

    oss << mTimeRange << " :" << std::endl;

    Json::StyledStreamWriter writer("   ");
    writer.write(oss, mData);

    return oss.str();
}

void
SearchResult::Parse(const Json::Value &v)
{
    mTimeRange = Json::TimeRangeFromJson(v["timerange"]);
    mData = v["data"];
}

void
SearchResult::Render(Json::Value &v) const
{
    Json::TimeRangeToJson(mTimeRange, v["timerange"]);
    v["data"] = mData;
}

std::string
SearchResultVector::Print() const
{
    std::ostringstream oss;
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        oss << i->Print() << std::endl;
    }

    return oss.str();
}

void
SearchResultVector::Parse(const Json::Value &v)
{
    if (!v.isArray())
        throw cor::Exception("SearchResultVector::Parse() -- expected array");

    Json::ValueConstIterator i = v.begin();
    for (; i != v.end(); i++)
    {
        push_back(SearchResult());
        back().Parse(*i);
    }
}

void
SearchResultVector::Render(Json::Value &v) const
{
    if (empty())
    {
        v = Json::Value();
        return;
    }

    for (int i = 0; i < size(); i++)
    {
        (*this)[i].Render(v[i]);
    }
}

IOOperation::IOOperation(const cor::TimeRange& timeRange,
                         IterationDirection direction,
                         const muuid::uuid& handle) :
        mTimeRange(timeRange),
        mDirection(direction),
        mHandle(handle)
{
    /*printf("IOOperation::IOOperation() %s%s\n",
           handle.string().c_str(),
           direction == eIterateBackwards ? " (backwards)" : "");*/
    LeakDetector::Get().Increment("IOOperation");
}

IOOperation::~IOOperation()
{
    //printf("IOOperation::~IOOperation() %s\n", mHandle.string().c_str());
    LeakDetector::Get().Decrement("IOOperation");
}

cor::TimeRange
IOOperation::GetTimeRange() const
{
    return mTimeRange;

}
muuid::uuid
IOOperation::GetHandle() const
{
    return mHandle;
}

/** Class ReadIOPThread :
*
*/
class ReadIOPThread : public cor::Thread
{
public:
    ReadIOPThread(nabu::IOOperationPtr readIOP,
                  const MetaKey& column,
                  RecordVector& recordVector);
    virtual ~ReadIOPThread();

    void SetTimeRanges(const std::set<cor::TimeRange>& trs) { mTimeRangeSet = trs; }

    void Assert() const
    {
        if (mException)
            throw mException;
    }

private:
    nabu::IOOperationPtr mReadIOP;
    const nabu::MetaKey mColumn;
    RecordVector& mRecordVector;
    std::set<cor::TimeRange> mTimeRangeSet;

    cor::Exception mException;

    void ThreadFunction();
};

ReadIOPThread::ReadIOPThread(nabu::IOOperationPtr readIOP,
                             const MetaKey& column,
                             RecordVector& recordVector) :
        cor::Thread("ReadIOPThread"),
        mReadIOP(readIOP),
        mColumn(column),
        mRecordVector(recordVector)
{
    LeakDetector::Get().Increment("ReadIOPThread");
}

ReadIOPThread::~ReadIOPThread()
{
    LeakDetector::Get().Decrement("ReadIOPThread");
}

void
ReadIOPThread::ThreadFunction()
{
    try
    {
        mRecordVector.clear();
        if (mTimeRangeSet.empty())
        {
            mReadIOP->Read(mColumn, mRecordVector);
        }
        else
        {
            std::set<cor::TimeRange>::const_iterator i = mTimeRangeSet.begin();
            for (; i != mTimeRangeSet.end(); i++)
            {
                mReadIOP->ReadSeek(mColumn, i->First());
                RecordVector rv;
                mReadIOP->Read(mColumn, rv, 0, i->Final());
                mRecordVector.Append(rv);
            }
        }
    } catch (const cor::Exception& err)
    {
        printf("Error reading column '%s': %s\n",
               mColumn.GetLiteral().c_str(), err.what());
        mException = err;
    }

}

void
IOOperation::Read(nabu::AccessPtr access,
                  IterationDirection direction,
                  const std::vector<MetaKey>& columns,
                  std::vector<RecordVector>& records,
                  const cor::TimeRange& timeRange)
{
    // DEFENSIVE
    if (columns.size() != records.size())
        throw cor::Exception("IOOperation::Read() -- columns (%ld) and records (%ld) must be same size",
                             columns.size(),
                             records.size());

    if (columns.empty())
        return; // nothing to do

    std::vector<std::shared_ptr<ReadIOPThread>> threads(columns.size());

    for (size_t i = 0; i < columns.size(); i++)
    {
        threads[i].reset(new ReadIOPThread(access.CreateIOOperation(timeRange, direction),
                                           columns[i],
                                           records[i]));
        threads[i]->Start();
    }

    while(1)
    {
        size_t i;
        for (i = 0; i < threads.size(); i++)
        {
            if (threads[i]->IsRunning())
            {
                // at least one thread is not done yet; wait a bit and
                // try again
                usleep(10000);
                break;
            }
        }
        if (i == threads.size()) // done?
            break;
    }

    // make sure join is called to avoid leaking threads
    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Stop();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Assert();
}

void
IOOperation::Read(nabu::AccessPtr access,
                  IterationDirection direction,
                  nabu::RecordMap& recordMap,
                  const cor::TimeRange& timeRange)
{
    if (recordMap.empty())
        return; // nothing to do

    std::vector<std::shared_ptr<ReadIOPThread>> threads(recordMap.size());

    nabu::RecordMap::iterator rmi = recordMap.begin();
    for (size_t i = 0; rmi != recordMap.end(); rmi++, i++)
    {
        threads[i].reset(new ReadIOPThread(access.CreateIOOperation(timeRange, direction),
                                           rmi->first,
                                           rmi->second));
        threads[i]->Start();
    }

    while(1)
    {
        size_t i;
        for (i = 0; i < threads.size(); i++)
        {
            if (threads[i]->IsRunning())
            {
                // at least one thread is not done yet; wait a bit and
                // try again
                usleep(10000);
                break;
            }
        }
        if (i == threads.size()) // done?
            break;
    }

    // make sure join is called to avoid leaking threads
    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Stop();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Assert();
}

void
IOOperation::Read(nabu::AccessPtr access,
                  IterationDirection direction,
                  const WrittenColumnsMap& wcm,
                  RecordMap& records)
{
    if (wcm.empty())
        return; // nothing to do

    cor::ThreadSet<ReadIOPThread> threads;

    nabu::WrittenColumnsMap::const_iterator rmi = wcm.begin();
    for (; rmi != wcm.end(); rmi++)
    {
        if (rmi->second.empty())
            continue;

        threads.Add(new ReadIOPThread(access.CreateIOOperation(access->GetTimeRange(), direction),
                                           rmi->first,
                                           records[rmi->first]));
    }

    threads.WaitUntilDone();
}

bool
IOOperation::Read(const MetaKey& column,
          Record& record,
          cor::Time until)
{
    RecordVector rv;
    Read(column, rv, 1, until);

    if (!rv.empty())
        record = rv.front();
    return !rv.empty();
}

void
IOOperation::Read(const MetaKey& column,
                  const SearchResultVector& which,
                  RecordVector& records)
{
    for (size_t i = 0; i < which.size(); i++)
    {
        ReadSeek(column, which[i].mTimeRange.First());
        Read(column, records, 0, which[i].mTimeRange.Final());
    }
}


/** Class WriteIOPThread :
*
*/
class WriteIOPThread : public cor::Thread
{
public:
    WriteIOPThread(nabu::IOOperation& writeIOP,
                   const MetaKey& column,
                   const RecordVector& recordVector);
    virtual ~WriteIOPThread();

    void Assert() const
    {
        if (mException)
            throw mException;
    }

private:
    nabu::IOOperation& mWriteIOP;
    const nabu::MetaKey mColumn;
    const std::string mType;
    const RecordVector& mRecordVector;

    cor::Exception mException;

    void ThreadFunction();
};


WriteIOPThread::WriteIOPThread(nabu::IOOperation& writeIOP,
                               const MetaKey& column,
                               const RecordVector& recordVector) :
        cor::Thread("WriteIOPThread"),
        mWriteIOP(writeIOP),
        mColumn(column),
        mRecordVector(recordVector)
{
    LeakDetector::Get().Increment("WriteIOPThread");
}

WriteIOPThread::~WriteIOPThread()
{
    LeakDetector::Get().Decrement("WriteIOPThread");
}

void
WriteIOPThread::ThreadFunction()
{
    try
    {
        // write to database
        mWriteIOP.Write(mColumn, mRecordVector);
        mWriteIOP.Commit();
    } catch (const cor::Exception& err)
    {
        printf("Error writing to column '%s': %s\n",
               mColumn.GetLiteral().c_str(), err.what());
        mException = err;
    }

}

void
IOOperation::Write(const MetaKey& column, const Record& record)
{
    RecordVector rv;
    rv.push_back(record);

    Write(column, rv);
}

void
IOOperation::Write(const std::vector<MetaKey>& columns, const std::vector<RecordVector>& records)
{
    // DEFENSIVE
    if (columns.size() != records.size())
        throw cor::Exception("IOOperation::Write() -- columns and records must be same size");

    if (columns.empty())
        return; // nothing to do

    std::vector<std::shared_ptr<WriteIOPThread>> threads(columns.size());

    for (size_t i = 0; i < columns.size(); i++)
    {
        threads[i].reset(new WriteIOPThread(*this, columns[i], records[i]));
        threads[i]->Start();
    }

    while(1)
    {
        size_t i;
        for (i = 0; i < threads.size(); i++)
        {
            if (threads[i]->IsRunning())
            {
                // at least one thread is not done yet; wait a bit and
                // try again
                usleep(10000);
                break;
            }
        }
        if (i == threads.size()) // done?
            break;
    }

    // make sure join is called to avoid leaking threads
    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Stop();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Assert();
}

void
IOOperation::Write(nabu::AccessPtr access,
                   IterationDirection direction,
                   const std::vector<MetaKey>& columns,
                   const std::vector<RecordVector>& records)
{
    // DEFENSIVE
    if (columns.size() != records.size())
        throw cor::Exception("IOOperation::Write() -- columns and records must be same size");

    if (columns.empty())
        return; // nothing to do

    std::vector<std::shared_ptr<WriteIOPThread>> threads(columns.size());
    std::vector<nabu::IOOperationPtr> iops(columns.size());

    //printf("%ld columns to write\n", columns.size());

    try
    {
        for (size_t i = 0; i < columns.size(); i++)
        {
            if (records[i].empty())
            {
                //printf("Skipping empty column %s\n", columns[i].GetLiteral().c_str());
                continue;
            }
            cor::TimeRange tr(records[i].front().GetTime(), records[i].back().GetTime());
            //printf("%s %ld records\n", columns[i].GetLiteral().c_str(), tr.IntegerSecondCount());
            iops[i] = access.CreateIOOperation(tr, direction);

            threads[i].reset(new WriteIOPThread(*(iops[i].get()), columns[i], records[i]));
            threads[i]->Start();
        }
    } catch (const cor::Exception& err)
    {
        printf("Error writing: %s\n", err.what());
        throw;
    }

    while(1)
    {
        size_t i;
        for (i = 0; i < threads.size(); i++)
        {
            // there was no thread started for a columns with no data
            if (!threads[i])
                continue;

            if (threads[i]->IsRunning())
            {
                // at least one thread is not done yet; wait a bit and
                // try again
                usleep(10000);
                break;
            }
        }
        if (i == threads.size()) // done?
            break;
    }
    //printf("Writes are complete\n");

    // make sure join is called to avoid leaking threads
    for (size_t i = 0; i < threads.size(); i++)
    {
        if (threads[i])
            threads[i]->Stop();
    }

    // destructors cause deletion of far end
    iops.clear();

    for (size_t i = 0; i < threads.size(); i++)
    {
        if (threads[i])
            threads[i]->Assert();
    }

}

void
IOOperation::Write(const std::map<MetaKey, RecordVector>& recordMap)
{
    if (recordMap.empty())
        return;

    std::vector<std::shared_ptr<WriteIOPThread>> threads(recordMap.size());

    size_t t = 0;
    for (auto i = recordMap.begin(); i != recordMap.end(); i++, t++)
    {
        threads[t].reset(new WriteIOPThread(*this, i->first, i->second));
        threads[t]->Start();
    }

    while(1)
    {
        size_t i;
        for (i = 0; i < threads.size(); i++)
        {
            if (threads[i]->IsRunning())
            {
                // at least one thread is not done yet; wait a bit and
                // try again
                usleep(10000);
                break;
            }
        }
        if (i == threads.size()) // done?
            break;
    }

    // make sure join is called to avoid leaking threads
    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Stop();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Assert();
}

void
IOOperation::Write(nabu::AccessPtr access,
                   IterationDirection direction,
                   const RecordMap& recordMap,
                   const RangeMap& rangeMap)
{
    if (recordMap.empty())
        return;

    std::vector<std::shared_ptr<WriteIOPThread>> threads(recordMap.size());

    size_t t = 0;
    std::vector<IOOperationPtr> iops(recordMap.size());
    for (auto i = recordMap.begin(); i != recordMap.end(); i++, t++)
    {
        iops[t] = access.CreateIOOperation(rangeMap.at(i->first), direction);
        threads[t].reset(new WriteIOPThread(*iops[t], i->first, i->second));
        threads[t]->Start();
    }

    while(1)
    {
        size_t i;
        for (i = 0; i < threads.size(); i++)
        {
            if (threads[i]->IsRunning())
            {
                // at least one thread is not done yet; wait a bit and
                // try again
                usleep(10000);
                break;
            }
        }
        if (i == threads.size()) // done?
            break;
    }

    // make sure join is called to avoid leaking threads
    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Stop();

    for (size_t i = 0; i < threads.size(); i++)
        threads[i]->Assert();
}

void
IOOperation::WriteRegions(const std::map<MetaKey, RecordVector>& recordMap)
{
}


}
