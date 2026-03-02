//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <string.h>

#include <openssl/md5.h>

#include <mcor/binary.h>

#include "branch.h"
#include "data_file.h"
#include "database_imp.h"
#include "filenamer.h"
#include "journal.h"
#include "leak_detector.h"
#include "profiler.h"
#include "version.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

const uint32_t normalVersion = 1;
const uint32_t copiedVersion = 2;

namespace nabu {

const unsigned char magic[] = {0x4d, 0x49, 0x4e, 0x41};

JournalImp::JournalImp(BranchImp* branch, const MetaKey& column) :
    mColumnAddress(column),
    mBranch(branch),
    mMutex("JournalImp")
{
    LeakDetector::Get().Increment("JournalImp");
}

// again, this constructor only used for utility of parsing a file
JournalImp::JournalImp(const std::string& fileName) :
    mBranch(NULL),
    mMutex("JournalImp")
{
    LeakDetector::Get().Increment("JournalImp");
    ParseFile(fileName);
}

JournalImp::~JournalImp()
{
    LeakDetector::Get().Decrement("JournalImp");
}

void
JournalImp::Read(const cor::TimeRange& timeRange, RecordVector& records)
{
    cor::ObjectLocker ol(mMutex);

    // DEFENSIVE
    if (!timeRange.Valid())
        throw cor::Exception("JournalImp::Read() -- 'Invalid' time range passed");

    records.clear();

    RecordMap::const_iterator i = mRecords.lower_bound(timeRange.First());
    if (i == mRecords.end())
        return;
    i++; // assumes exclusive beginning of timeRange, because it was already read

    RecordMap::const_iterator end = mRecords.upper_bound(timeRange.Final());

    // can't tell how many records exist between iterators, so add them
    // individually
    for (; i != end; i++)
        records.push_back(i->second);
}

bool
JournalImp::Read(const cor::Time& time, Record& records)
{
    cor::ObjectLocker ol(mMutex);

    RecordMap::const_iterator i = mRecords.find(time);
    if (i == mRecords.end())
        return false;
    records = i->second;
    return true;
}

void
JournalImp::Append(RecordVector& records)
{
    DEBUG_LOCAL("JournalImp::Append() -- appending %ld records to %s\n", records.size(), mColumnAddress.GetLiteral().c_str());
    cor::ObjectLocker ol(mMutex);

    Version version;
    version.AsNumber(normalVersion);
    std::string fileName = mBranch->GetDatabaseImp().GetFileNamer().NameOf(mColumnAddress, TimePath(), version, FileType::eJournal);

    // create the file, if necessary
    if (!cor::File::Exists(fileName))
    {
        DEBUG_LOCAL("JournalImp::Append() -- creating new file '%s'\n", fileName.c_str());
        cor::File::MakePath(fileName);
        cor::File file(fileName, O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IROTH);
        WriteHeader(file);
    }

    size_t skipped = 0;
    {
        cor::File file(fileName, O_WRONLY | O_APPEND);

        cor::Time last = GetLastTime();
        for (size_t i = 0; i < records.size(); i++)
        {
            // DEFENSIVE -- ignore any records newer than the last one held already
            if (last.Valid() && (records[i].GetTime() <= last))
            {
                skipped++;
                continue;
            }
            mRecords[records[i].GetTime()] = records[i];
            WriteRecord(file, records[i]);
        }

        DEBUG_LOCAL("JournalImp::Append() -- wrote %ld records, skipped %ld\n", records.size() - skipped, skipped);
        file.Sync();
    }

    if (skipped != 0)
    {
        printf("JournalImp::Write() -- Warning, skipped %ld stale records writing to %s\n",
               skipped, mColumnAddress.GetLiteral().c_str());
    }
}

cor::TimeRange
JournalImp::Extents() const
{
    cor::ObjectLocker ol(mMutex);
    if (mRecords.empty())
        return cor::TimeRange(); // invalid

    RecordMap::const_iterator back = mRecords.end();
    back--;
    return cor::TimeRange(mRecords.begin()->first, back->first);
}

/* File format
 *
 * MAGIC                         <4 bytes> 0x4D494E41 ("MINA")
 * VERSION                       <2 bytes> 0x00000001
 *
 * (version 1)
 * <Record 1>
 * <Record 2>
 * ...
 *
 * Record Format (version 1)
 * UTC SECONDS + NANOSECONDS    <4 bytes + 4 bytes>
 * RECORD SIZE                  <4 bytes>
 * DATA
 * MD5 of record                <16 bytes>
 *
 */

void
JournalImp::Load()
{
    GetProfiler().Count("JournalImp::Load");

    std::string glob = mBranch->GetDatabaseImp().GetFileNamer().JournalGlob(mBranch->GetBranchPath(), mColumnAddress);

    std::vector<std::string> fileNames;
    cor::File::FindFilenames(glob, fileNames);

    if (fileNames.empty())
    {
        DEBUG_LOCAL("No files found for journal '%s'", mColumnAddress.GetLiteral().c_str());
        return;
    }

    for (size_t j = 0; j < fileNames.size(); j++)
    {
        ParseFile(fileNames[j]);
    }

    DEBUG_LOCAL("JournalImp::Read %ld records for %s from %ld files\n",
                mRecords.size(), mColumnAddress.GetLiteral().c_str(), fileNames.size());
}

void
JournalImp::ParseFile(const std::string& filename)
{
    cor::File file(filename, O_RDONLY);

    size_t i = 0;

    try
    {
        unsigned char buffer[20];
        size_t n = file.Read((char*)buffer, 4);

        if ((n != 4) || (strncmp((char*)buffer, (char*)magic, sizeof(magic)) != 0))
            throw cor::Exception("Invalid MAGIC");

        n = file.Read((char*)buffer, 2);
        if ((n != 2))
            throw cor::Exception("EOF encountered reading VERSION");
        unsigned int version = cor::Unpack16(buffer);
        if (version != 1)
            throw cor::Exception("Unsupported data file version %d", version);

        while (true)
        {
            // compute md5
            MD5_CTX md5Ctx;
            MD5_Init(&md5Ctx);

            // read timestamp
            const size_t timeStampSize = 16; // two 64-bit values, sec and asec
            n = file.Read((char*)buffer, timeStampSize);

            // if journal is complete and written cleanly, this is the expected
            // EOF
            if (n == 0)
            {
                break;
            }

            if (n != timeStampSize)
                throw cor::Exception("EOF encountered reading timestamp in record %d", i);

            int64_t sec = cor::Unpack64(buffer);
            int64_t asec = cor::Unpack64(buffer + 8);
            MD5_Update(&md5Ctx, buffer, timeStampSize);

            n = file.Read((char*)buffer, 4);
            if (n != 4)
                throw cor::Exception("EOF encountered reading size for record %d", i);
            size_t recordSize = cor::Unpack32(buffer);
            MD5_Update(&md5Ctx, buffer, 4);

            Record record;
            // XXX need to support TRR type as well here
            record.SetTime(cor::Time(sec, asec));
            record.mData.reset(new std::vector<unsigned char>);
            record.mData->resize(recordSize);

            n = file.Read((char*)record.mData->data(), recordSize);
            if (n != recordSize)
                throw cor::Exception("EOF encountered reading data for record %d", i);
            MD5_Update(&md5Ctx, record.mData->data(), recordSize);

            n = file.Read((char*)buffer, 16);
            if (n != 16)
                throw cor::Exception("EOF encountered reading checksum");

            unsigned char md[MD5_DIGEST_LENGTH];
            MD5_Final(md, &md5Ctx);
            for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
            {
                if (md[i] != buffer[i])
                {
                    printf("Checksum error in record %ld, skipping\n", i);
                }
            }

            mRecords[record.GetTime()] = record;
        }
    } catch (const cor::Exception& err)
    {
        printf("Incomplete or corrupted journal file '%s': %s\n", filename.c_str(), err.what());
        printf("%ld records recovered\n", i);
    }
}

// clear all records up to and including final, rendering a new
// journal file
void
JournalImp::Clear(const cor::Time& final)
{
    cor::ObjectLocker ol(mMutex);

    // remove the records from the collection
    {
        RecordMap::iterator fi = mRecords.upper_bound(final);
        if (fi == mRecords.end())
        {
            DEBUG_LOCAL("JournalImp::Clear() -- no records to clear from %s", mColumnAddress.GetLiteral().c_str());
        }
        mRecords.erase(mRecords.begin(), fi);
    }

    GetProfiler().Count("JournalImp::Clear");

    // render a new journal file of the remaining records
    TimePath tpIgnored;
    Version version;
    version.AsNumber(copiedVersion);
    std::string fileName =
            mBranch->GetDatabaseImp().GetFileNamer().NameOf(mColumnAddress,
                                                         tpIgnored,
                                                         version,
                                                         FileType::eJournal);

    cor::File::MakePath(fileName);
    cor::File file(fileName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IROTH);

    WriteHeader(file);

    RecordMap::const_iterator i = mRecords.begin();
    for (size_t n = 0; i != mRecords.end(); i++, n++)
    {
        const Record& r = i->second;
        try
        {
            WriteRecord(file, r);
        }
        catch (const cor::Exception& err)
        {
            // append record number and re-throw
            throw cor::Exception("%s : journal record %ld in %s",
                                 err.what(),
                                 n,
                                 fileName.c_str());
        }
    }

    DEBUG_LOCAL("DataFile::Write of %ld records to %s complete\n", mRecords.size(), fileName.c_str());

    version.AsNumber(normalVersion);
    std::string standardName =
            mBranch->GetDatabaseImp().GetFileNamer().NameOf(mColumnAddress,
                                                         tpIgnored,
                                                         version,
                                                         FileType::eJournal);
    // remove the old journal file
    cor::File::Delete(standardName);

    // rename the new journal file to the permanent name
    file.Rename(standardName);
}

void
JournalImp::WriteHeader(cor::File& file)
{
    size_t n = file.Write((char*)magic, 4);
    if (n != 4)
        throw cor::Exception("EOF writing magic");

    unsigned char buffer[20];
    cor::Pack16(buffer, 1);
    n = file.Write((char*)buffer, 2);
    if ((n != 2))
        throw cor::Exception("EOF writing VERSION");
}

void
JournalImp::WriteRecord(cor::File &file, Record r)
{
    MD5_CTX md5Ctx;
    MD5_Init(&md5Ctx);

    unsigned char buffer[20];
    cor::Pack64(buffer, r.GetTime().SecondPortion());
    cor::Pack64(buffer + 8, r.GetTime().AttoSecondPortion());
    size_t n = file.Write((char*)buffer, 8);
    if (n != 8)
        throw cor::Exception("EOF writing timestamp in record");
    MD5_Update(&md5Ctx, buffer, 8);

    cor::Pack32(buffer, r.mData->size());
    n = file.Write((char*)buffer, 4);
    if (n != 4)
        throw cor::Exception("EOF writing size for record");
    MD5_Update(&md5Ctx, buffer, 4);

    n = file.Write((char*)r.mData->data(), r.mData->size());
    if (n != r.mData->size())
        throw cor::Exception("EOF writing data for record");
    MD5_Update(&md5Ctx, r.mData->data(), r.mData->size());

    unsigned char md[MD5_DIGEST_LENGTH];
    MD5_Final(md, &md5Ctx);
    n = file.Write((char*)md, MD5_DIGEST_LENGTH);
    if (n != MD5_DIGEST_LENGTH)
        throw cor::Exception("EOF writing checksum");
}
    
}
