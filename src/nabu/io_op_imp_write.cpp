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
#include <algorithm>

#include "trace.h"

#include <mcor/profiler.h>

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

IOOperationImp::WriteImp::WriteImp() :
        mMutex("IOOperationImp"),
        mLastTime(cor::Time::eInvalid),
        mLastRegionEnd(cor::Time::eInvalid)
{
    LeakDetector::Get().Increment("IOOperationImp::WriteImp");
}

IOOperationImp::WriteImp::~WriteImp()
{
    LeakDetector::Get().Decrement("IOOperationImp::WriteImp");
}

void
IOOperationImp::WriteImp::UpdateWriteExtents(const Record& r)
{
    if (!mWrittenRange.First().Valid() ||
        (mWrittenRange.First() > r.mTimeRange.First()))
    {
        mWrittenRange.First() = r.mTimeRange.First();
    }
    if (!mWrittenRange.Final().Valid() ||
        (mWrittenRange.Final() < r.mTimeRange.Final()))
    {
        mWrittenRange.Final() = r.mTimeRange.Final();
    }
}

void
IOOperationImp::GetWrittenColumns(WrittenColumnsMap& writtenColumnsMap, cor::TimeRange& extents)
{
    cor::ObjectLocker ol(mWriteImpMutex);
    writtenColumnsMap = mSelectMap;
    extents = mSelectTimeRange;
}

void
IOOperationImp::Write(const MetaKey& column, const RecordVector& records)
{
    DEBUG_LOCAL("IOOperation::Write %s %zu records\n", column.GetLiteral().c_str(), records.size());

    //if (ci.mRecordType == ePITR)
    /*{
        // check for out-of-range (in time) data
        cor::TimeRange tr;
        if (!records.empty())
        {
            if (mDirection == eIterateForwards)
                tr = cor::TimeRange(records.front().mTimeRange.First(), records.back().mTimeRange.Final());
            else
                tr = cor::TimeRange(records.back().mTimeRange.First(), records.front().mTimeRange.Final());
        }

        // Note that this assumes extents of time are at extremes; out-of-order data could defeat
        // this, but is checked later and throws its own exception.
        if (!tr.Within(mTimeRange))
            throw cor::Exception("Time range of data to be written (%s) exceeds that of IOOperation (%s)",
                tr.Print().c_str(),
                mTimeRange.Print().c_str());

        // do the write
        WriteImpl(column, records);
    }
    else if (ci.mRecordType == eTRR)*/
    {
        // overlap and out-of-bounds testing done in WriteImpl

        // Test if initial existing TRR overlaps this timerange, but extends
        // before the beginning.
        // This is illegal because it would violate coherency by changing
        // the state of data outside the time range of the IO operation
        Record firstExistingTRR;
        if (FetchLastRecord(column, mTimeRange.First(), firstExistingTRR, eInclusive))
        {
            // XXX alternative policy is to do nothing with this first TRR
            if (mTimeRange.Intersects(firstExistingTRR.mTimeRange) &&
                    firstExistingTRR.mTimeRange.First() < mTimeRange.First())
            {
                throw cor::Exception("IOOperation::Write() -- existing TRR record in %s ending at %s has start time %s "
                                     "which preceeds that of the IO operation (%s), but would be interfered by this write",
                                     column.GetLiteral().c_str(),
                                     firstExistingTRR.mTimeRange.Final().Print().c_str(),
                                     firstExistingTRR.mTimeRange.First().Print().c_str(),
                                     mTimeRange.Print().c_str());
            }
        }

        // Test if final existing TRR overlaps this timerange, but extends
        // past the end.
        // This is illegal because it would violate coherency by changing
        // the state of data outside the time range of the IO operation
        Record lastExistingTRR;
        if (FetchLastRecord(column, mTimeRange.Final(), lastExistingTRR, eInclusive))
        {
            // XXX alternative policy is to do nothing with this final TRR
            if (mTimeRange.Intersects(lastExistingTRR.mTimeRange) &&
                lastExistingTRR.mTimeRange.Final() > mTimeRange.Final())
            {
                throw cor::Exception("IOOperation::Write() -- existing TRR record in %s at %s has end time %s "
                                     "which exceeds that of the IO operation (%s), but would be cleared by this write",
                                     column.GetLiteral().c_str(),
                                     lastExistingTRR.mTimeRange.First().Print().c_str(),
                                     lastExistingTRR.mTimeRange.Final().Print().c_str(),
                                     mTimeRange.Print().c_str());
            }
        }

        IOOperationImp::WriteImpl(column, records);
    }
}

void
IOOperationImp::WriteImpl(const MetaKey& column, const RecordVector& recordsIn)
{
    DEBUG_LOCAL("IOOperationImp::Write -- %s %ld records\n",
                column.GetLiteral().c_str(), recordsIn.size());
    cor::TimeRange tr; // used only for Context statement below
    if (!recordsIn.empty())
        tr = cor::TimeRange(recordsIn.front().mTimeRange.First(), recordsIn.back().mTimeRange.Final());
    Context ctx("IOOperation::Write %s : %s, %ld records over timeRange %s",
                mHandle.string().c_str(),
                column.GetLiteral().c_str(),
                recordsIn.size(),
                tr.Print().c_str());

    if (!mAccessImp->IsWrite())
        throw cor::Exception("Write() -- Access for operation does not allow writes");
    
    // do not allow write to tag
    if (!GetTagName().empty())
        throw cor::Exception("IOOperation -- cannot write data to a tag ('%s')", GetTagName().c_str());

    try {
        cor::ObjectLocker ol(mWriteImpMutex);
        WriteImp* imp = &mWriteImpMap[column];
        cor::ObjectLocker iol(imp->mMutex);
        ol.Unlock();

        // if this is backwards data, then just make a copy and reverse it; the rest of this function
        // assumes forward-oriented data
        RecordVector reversed;
        const RecordVector* records = &recordsIn;
        if (mDirection == eIterateBackwards)
        {
            std::reverse_copy(recordsIn.begin(), recordsIn.end(), std::back_inserter(reversed));
            records = &reversed;
        }

        // find the last moment in the file for the initial record
        cor::Time finalInFile;
        size_t begin = 0;
        size_t end = records->size();
        if (!records->empty())
        {
            if (!records->front().GetTime().Valid() || records->front().GetTime().IsInfinite())
                throw cor::Exception("IOOperation::Write() -- column %s, Invalid time value (%s) in records\n",
                                     column.GetLiteral().c_str(),
                                     records->front().GetTime().Print().c_str());

            // cannot write backwards
            if (imp->mLastTime.Valid() && (records->front().GetTime() <= imp->mLastTime))
            {
                throw cor::Exception("IOOperation::Write() -- attempted to write backwards in time in column %s (%s after %s)",
                    column.GetLiteral().c_str(),
                    records->front().GetTime().Print().c_str(),
                    imp->mLastTime.Print().c_str());
            }

            cor::Time firstInFile = mDataBase.GetTimeScheme().SnapFirst(records->front().GetTime());
            finalInFile = mDataBase.GetTimeScheme().SnapFinal(records->front().GetTime());

            // copy leading data, if necessary
            if (!mTimeRange.In(firstInFile))
            {
                for (; begin != records->size(); begin++)
                {
                    const Record& r = (*records)[begin];

                    if (r.GetTime() > finalInFile)
                        break;
                    imp->mLeadingEdge.push_back(r);
                    imp->UpdateWriteExtents(r);
                }

                // SANITY
                if (begin == 0)
                    throw FatalException(GetBranchImp()->GetLiteral(),
                                         "IOOperation::Write() -- no leading data");

                if (!imp->mLeadingEdge.empty())
                {
                    DEBUG_LOCAL("IOOperation::Write() -- leading data is from %s to %s\n",
                           imp->mLeadingEdge.front().GetTime().Print().c_str(),
                           imp->mLeadingEdge.back().GetTime().Print().c_str());
                }
            }

            cor::Time finalInFinalFile = mDataBase.GetTimeScheme().SnapFinal(records->back().GetTime());

            // copy trailing data, if necessary
            if (!mTimeRange.In(finalInFinalFile))
            {
                end = begin;
                cor::Time firstInFinalFile = mDataBase.GetTimeScheme().SnapFirst(records->back().GetTime());
                for (; end != records->size(); end++)
                {
                    if ((*records)[end].GetTime() >= firstInFinalFile)
                        break;
                }

                // end points to first record in the final file

                for (size_t i = end; i != records->size(); i++)
                {
                    const Record& r = (*records)[i];

                    // check for out-of-bounds record
                    if (!mTimeRange.In(r.GetTime()))
                    {
                        throw cor::Exception("%s: %s -- time in record (%s) is outside of write access range (%s)",
                                             mHandle.string().c_str(),
                                             column.GetLiteral().c_str(),
                                             r.GetTime().Print().c_str(),
                                             mTimeRange.Print().c_str());
                    }
                    imp->mTrailingEdge.push_back(r);
                    imp->UpdateWriteExtents(r);
                }

                if (!imp->mTrailingEdge.empty())
                {
                    DEBUG_LOCAL("IOOperation::Write() -- trailing data is from %s to %s\n",
                           imp->mTrailingEdge.front().GetTime().Print().c_str(),
                           imp->mTrailingEdge.back().GetTime().Print().c_str());
                }
            }

            // handle case that the last record written (by a previous Write) was
            // the last record for its file, but did not get put to disk yet (because
            // the loop below looks for crossing the file boundary, and it had not)
            if (imp->mLastTime.Valid() && (imp->mLastTime < firstInFile))
            {
                FlushToFile(column, imp, imp->mWriteBuffer.front().GetTime());
            }
        }

        for (size_t i = begin; i < end; i++)
        {
            const Record& r = (*records)[i];
            if (!r.GetTime().Valid() || r.GetTime().IsInfinite())
                throw cor::Exception("Invalid time value (%s) in records\n", r.GetTime().Print().c_str());

            if (imp->mLastTime.Valid() && (r.GetTime() <= imp->mLastTime))
            {
                throw cor::Exception("%s: %s -- Non-increasing time value in write (current = %s, last = %s)",
                                     mHandle.string().c_str(),
                                     column.GetLiteral().c_str(),
                                     r.GetTime().Print().c_str(),
                                     imp->mLastTime.Print().c_str());
            }
            // check for out-of-bounds record
            if (!mTimeRange.In(r.GetTime()))
            {
                throw cor::Exception("%s: %s -- time in record (%s) is outside of write access range (%s)",
                                     mHandle.string().c_str(),
                                     column.GetLiteral().c_str(),
                                     r.GetTime().Print().c_str(),
                                     mTimeRange.Print().c_str());
            }
            if (r.GetTime() > finalInFile)
            {
                if (!imp->mWriteBuffer.empty())
                {
                    FlushToFile(column, imp, imp->mWriteBuffer.front().GetTime());
                }
                finalInFile = mDataBase.GetTimeScheme().SnapFinal(r.GetTime());
            }
            imp->UpdateWriteExtents(r);
            imp->mWriteBuffer.push_back(r);
            imp->mLastTime = r.GetTime();

            // if we have passed outside the time range for the write, we are done
            if (!mTimeRange.In(r.GetTime()))
            {
                break;
            }
        }
    }
    catch (...)
    {
        Abort();
        throw;
    }
}

void
IOOperationImp::Commit()
{
    DEBUG_LOCAL("IOOperation::Commit() -- %s\n", mHandle.string().c_str());
    Context ctx("IOOperation::Commit %s %s, access %s %s",
                mHandle.string().c_str(),
                mTimeRange.Print().c_str(),
                mAccessImp->GetHandle().string().c_str(),
                mAccessImp->GetTimeRange().Print().c_str());
    if (!mAccessImp->IsWrite())
        throw cor::Exception("Commit() -- Access for operation does not allow writes");
    
    if (!GetTagName().empty())
    {
        // do not allow ordinary commit to tag
        throw cor::Exception("IOOperation -- cannot commit to a tag ('%s')", GetTagName().c_str());
    }

    size_t total = 0;
    cor::ObjectLocker ol(mWriteImpMutex);

    WriteImpMap::iterator i = mWriteImpMap.begin();
    while (i != mWriteImpMap.end())
    {
        // writes to a single column within an Access need FlushToFile,
        // FlushEdges, and Branch->Commit to be atomic
        cor::ObjectLocker columnLock(mAccessImp->GetColumnMutex(i->first));

        DEBUG_LOCAL("   > Commiting %s\n", i->first.GetLiteral().c_str());
        WriteImp& imp = i->second;
        {
            cor::ObjectLocker iol(imp.mMutex);

            // note the range written to this column, if anything was
            if (i->second.mWrittenRange.Valid())
            {
                mSelectMap[i->first].insert(i->second.mWrittenRange);
                mSelectTimeRange = mSelectTimeRange.SuperUnion(i->second.mWrittenRange);
                /*printf("Commit() -- adding written range %s into %s\n",
                       i->second.mWrittenRange.Print().c_str(),
                       i->first.GetLiteral().c_str());*/
            }
            else
            {
                //printf("Commit() -- Nothing actually written to column %s\n", i->first.GetLiteral().c_str());
            }

            CommitColumn(i->first);
        }
        // use post-increment because pre-C++11 no iterator is returned
        mWriteImpMap.erase(i++);
    }
    DEBUG_LOCAL("IOOperation::Commit() -- %s committed %ld files\n", mHandle.string().c_str(), total);
}

void
IOOperationImp::CommitColumn(const MetaKey& column)
{
    // both map mutex and column mutex should be held during this call
    DEBUG_LOCAL("IOOperation::CommitColumn() -- %s\n", mHandle.string().c_str());
    Context ctx("IOOperation::CommitColumn %s %s",
                mHandle.string().c_str(),
                column.GetLiteral().c_str());

    WriteImp& imp = mWriteImpMap[column];

    size_t total = 0;
    if (!imp.mWriteBuffer.empty())
    {
        DEBUG_LOCAL("IOOperation::CommitColumn() -- Committing final data: %s to %s\n",
                    imp.mWriteBuffer.front().GetTime().Print().c_str(),
                    imp.mWriteBuffer.back().GetTime().Print().c_str());
        FlushToFile(column, &imp, imp.mWriteBuffer.front().GetTime());
    }

    try
    {
        FlushEdges(column);
        GetBranchImp()->Commit(column, imp.mNewFiles, mTimeRange);
    } catch (const cor::Exception& err)
    {
        printf("Exception committing column %s\n", column.GetLiteral().c_str());
        printf("IO handle %s\n", mHandle.string().c_str());
        printf("Access handle %s\n", mAccessImp->GetHandle().string().c_str());
        printf("written range %s\n", imp.mWrittenRange.Print().c_str());
        printf("IO range %s\n", mTimeRange.Print().c_str());

        throw err;
    }

    for (auto j = imp.mOldFiles.begin(); j != imp.mOldFiles.end(); j++)
    {
        mDataBase.GetGarbage().Insert(j->second);
    }
    total += imp.mNewFiles.size();

    DEBUG_LOCAL("IOOperation::CommitImp() -- %s committed %ld files\n", mHandle.string().c_str(), total);
}


void
IOOperationImp::Abort()
{
    DEBUG_LOCAL("IOOperation::Abort() -- %s\n", mHandle.string().c_str());
    Context ctx("IOOperation::Abort %s", mHandle.string().c_str());
    // do nothing if this was opened on a tag; there was no data written anyway
    if (!GetTagName().empty())
        return;

    size_t total = 0;
    cor::ObjectLocker ol(mWriteImpMutex);

    WriteImpMap::iterator i = mWriteImpMap.begin();
    while (i != mWriteImpMap.end())
    {
        WriteImp& imp = i->second;
        {
            cor::ObjectLocker iol(imp.mMutex);

            std::map<TimePath, Garbage::Entry>::const_iterator j = imp.mNewFiles.begin();
            for (; j != imp.mNewFiles.end(); j++)
            {
                mDataBase.GetGarbage().Insert(j->second);
                total++;
            }
        }

        // use post-increment because pre-C++11 no iterator is returned
        mWriteImpMap.erase(i++);
    }

    DEBUG_LOCAL("IOOperationImp::Abort() %s aborted %ld files\n", mHandle.string().c_str(), total);
}

void
IOOperationImp::FlushEdges(const MetaKey& column)
{
    TimePath first = mDataBase.GetTimeScheme().GetPath(mTimeRange.First());
    TimePath final = mDataBase.GetTimeScheme().GetPath(mTimeRange.Final());
    DEBUG_LOCAL("FlushEdges: tr = %s, first = %s, final = %s\n",
                mTimeRange.Print().c_str(),
                first.GetLiteral().c_str(),
                final.GetLiteral().c_str());

    cor::ObjectLocker ol(mWriteImpMutex);

    // the first and final file are always written in order to deal with
    // edges. If they are not already in the writtenLeaves collection,
    // explicitly write them now.

    WriteImpMap::iterator i = mWriteImpMap.find(column);
    if (i != mWriteImpMap.end())
    {
        //cor::ObjectLocker iol(i->second.mMutex);
        //printf("FlushEdges: column %s\n", i->first.GetLiteral().c_str());

        if (i->second.mNewFiles.find(first) == i->second.mNewFiles.end())
        {
            //printf("Did not find write to front edge (%s) of %s: flushing it.\n",
            //            first.GetLiteral().c_str(), i->first.GetLiteral().c_str());
            i->second.mWriteBuffer.clear();
            FlushToFile(i->first, &(i->second), mTimeRange.First());
        }
        if (i->second.mNewFiles.find(final) == i->second.mNewFiles.end())
        {
            //printf("Did not find write to back edge (%s) of %s: flushing it.\n",
            //            final.GetLiteral().c_str(), i->first.GetLiteral().c_str());
            i->second.mWriteBuffer.clear();
            FlushToFile(i->first, &(i->second), mTimeRange.Final());
        }
    }
}

void
IOOperationImp::WriteAllToFile(WriteImp* imp,
                               const nabu::MetaKey& column,
                               const TimePath& timePath,
                               RecordVector& records)
{
    DEBUG_LOCAL("WriteAllToFile %s length = %ld\n",
                column.GetLiteral().c_str(),
                records.size());

    // DEFENSIVE
    if (!timePath.Valid())
    {
        throw cor::Exception("IOOperation::WriteAllToFile() -- called with invalid timePath\n");
    }
    cor::TimeRange tr;
    if (!records.empty())
        tr = cor::TimeRange(records.front().mTimeRange.First(), records.back().mTimeRange.Final());

    Context ctx("IOOperation::WriteAllToFile %s timepath %s records span %s",
                column.GetLiteral().c_str(),
                timePath.GetLiteral().c_str(),
                tr.Print().c_str());

    MetaDataTable root = GetRoot();
    Version version = root->GetCurrentVersion(mDataBase, column, timePath);

    TableAddress taNew(column, timePath, version.New());
    TableAddress taOld(column, timePath, version);
    std::string newFileName = mDataBase.GetFileNamer().NameOf(taNew, FileType::eData);
    std::string oldFileName = mDataBase.GetFileNamer().NameOf(taOld, FileType::eData);

    // this will cause erasure of the old file, so long as the IOOperation is
    // Committed later.
    imp->mOldFiles[taOld.GetTimePath()] = Garbage::Entry(taOld, FileType::eData);

    if (!records.empty())
    {
        // if there are records, create a new file with them
        imp->mNewFiles[taNew.GetTimePath()] = Garbage::Entry(taNew, FileType::eData);

        DEBUG_LOCAL("WriteAllToFile : file = '%s', %ld records from %s to %s\n",
                    newFileName.c_str(),
                    records.size(),
                    records.front().GetTime().Print().c_str(),
                    records.back().GetTime().Print().c_str());

        DataFile dataFile(newFileName);
        dataFile.Write(records, mDirection);
    }
}

void
IOOperationImp::FlushToFile(const MetaKey& column,
                            WriteImp* imp,
                            const cor::Time& timeOfData)
{
    Context ctx("IOOperation::FlushToFile %s", column.GetLiteral().c_str());
    DEBUG_LOCAL("IOOperationImp::FlushToFile() -- column %s, %ld records, time %s\n",
                column.GetLiteral().c_str(),
                imp->mWriteBuffer.size(),
                timeOfData.Print().c_str());

    // DEFENSIVE
    if (!timeOfData.Valid())
    {
        printf("!!!!! FlushToFile called with invalid timeOfData\n");
        return;
    }

    // all the data in the buffer goes to the same file (this is checked
    // during the Write, and CommitData is called just before that
    // is violated). So find filename appropriate for first record,
    // and write everything to that file

    TimePath path = mDataBase.GetPath(timeOfData);

    MetaDataTable root = GetRoot();
    Version version = root->GetCurrentVersion(mDataBase, column, path);

    TableAddress taNew(column, path, version.New());
    TableAddress taOld(column, path, version);
    std::string newFileName = mDataBase.GetFileNamer().NameOf(taNew, FileType::eData);
    std::string oldFileName = mDataBase.GetFileNamer().NameOf(taOld, FileType::eData);

    // need to stitch in data included in the first and final files that
    // are outside the range of the access so that it is not erased
    cor::TimeRange fileTimeRange = mDataBase.GetTime(path);

    //const ColumnInfo& columnInfo = mDataBase.GetColumnInfo(column);

    // fill in leading data, if necessary
    if (!mTimeRange.In(fileTimeRange.First()))
    {
        //printf("Reading existing data into write buffer from %s before %s\n", oldFileName.c_str(), mTimeRange.First().Print().c_str());
        FillBefore(oldFileName, imp->mWriteBuffer, mTimeRange.First(), cor::Time(cor::Time::eInvalid));
        //printf("got %ld records\n", imp->mWriteBuffer.size());

        const size_t n = imp->mWriteBuffer.size();
        imp->mWriteBuffer.resize(imp->mWriteBuffer.size() + imp->mLeadingEdge.size());

        // copy edge data to mWriteBuffer
        for (size_t i = 0; i < imp->mLeadingEdge.size(); i++)
        {
            imp->mWriteBuffer[n + i] = imp->mLeadingEdge[i];
        }
    }

    // fill in trailing data, if necessary
    if (!mTimeRange.In(fileTimeRange.Final()))
    {
        const size_t n = imp->mWriteBuffer.size();
        imp->mWriteBuffer.resize(imp->mWriteBuffer.size() + imp->mTrailingEdge.size());

        // copy edge data to mWriteBuffer
        for (size_t i = 0; i < imp->mTrailingEdge.size(); i++)
            imp->mWriteBuffer[n + i] = imp->mTrailingEdge[i];

        //printf("Reading existing data into write buffer from %s after %s\n", oldFileName.c_str(), mTimeRange.Final().Print().c_str());
        FillAfter(oldFileName, imp->mWriteBuffer, mTimeRange.Final(), cor::Time(cor::Time::eInvalid));
        //printf("got %ld records\n", imp->mWriteBuffer.size());
    }

    if (imp->mWriteBuffer.size() == 0)
    {
        //printf("Flush to file %s: no data\n", column.GetLiteral().c_str());
        return; // nothing to do
    }

    imp->mNewFiles[taNew.GetTimePath()] = Garbage::Entry(taNew, FileType::eData);
    imp->mOldFiles[taOld.GetTimePath()] = Garbage::Entry(taOld, FileType::eData);

    DEBUG_LOCAL("IOOperationImp::FlushToFile() -- writing file '%s', %ld records from %s to %s\n",
                newFileName.c_str(),
                imp->mWriteBuffer.size(),
                imp->mWriteBuffer.front().GetTime().Print().c_str(),
                imp->mWriteBuffer.back().GetTime().Print().c_str());
    DataFile dataFile(newFileName);
    dataFile.Write(imp->mWriteBuffer,eIterateForwards);
    imp->mWriteBuffer.clear();
}

void
IOOperationImp::FillBefore(const::std::string& oldFileName,
                              //const ColumnInfo& columnInfo,
                              RecordVector& records,
                              const cor::Time& first,
                              const cor::Time& start)
{
    DataFile existing(oldFileName);

    RecordVector existingRecords;
    if (existing.Exists())
    {
        // this is only called by FlushToFile, which has data in positive order
        existing.Read(existingRecords, eIterateForwards);

        // insert into records until we hit the beginning of the access range
        size_t i;
        size_t begin = 0;
        for (i = 0; i < existingRecords.size(); i++)
        {
            if (start.Valid() && existingRecords[i].GetTime() < start)
                begin = i + 1;
            if (existingRecords[i].GetTime() >= first)
                break;
        }

        // from start to i need to be copied into the front of records
        if (i != begin)
        {
            records.insert(records.begin(), existingRecords.begin() + begin, existingRecords.begin() + i);
            /*printf("Filled %ld before, from %s to %s\n",
                        i - begin,
                        (existingRecords.begin() + begin)->GetTime().Print(true).c_str(),
                        (existingRecords.begin() + i - 1)->GetTime().Print(true).c_str());*/
        }
    }
}

void
IOOperationImp::FillAfter(const::std::string& oldFileName,
                          //const ColumnInfo& columnInfo,
                          RecordVector& records,
                          const cor::Time& final,
                          const cor::Time& stop)
{
    DataFile existing(oldFileName);
    RecordVector existingRecords;
    if (existing.Exists())
    {
        // this is only called by FlushToFile, which has data in positive order
        existing.Read(existingRecords, eIterateForwards);

        int i;
        size_t end = existingRecords.size();
        for (i = existingRecords.size() - 1; i >= 0; i--)
        {
            if (stop.Valid() && existingRecords[i].GetTime() > stop)
                end = i;
            if (existingRecords[i].GetTime() <= final)
                break;
        }
        i++;

        // from i to end need to be copied into the back of records
        if (i != existingRecords.size())
        {
            records.insert(records.end(), existingRecords.begin() + i, existingRecords.begin() + end);
        }
    }
}


}

