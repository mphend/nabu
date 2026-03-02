//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <string.h>
#include <stdio.h>

#include <openssl/md5.h>

#include <mcor/binary.h>
#include <mcor/strutil.h>
#include <json/json.h>
#include <json/writer.h>
#include <zlib.h>

#include "exception.h"
#include "context.h"
#include "data_file.h"
#include "leak_detector.h"
#include "profiler.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

#define JSON_SIZE_LIMIT 100000
#define RECORD_SIZE_LIMIT 100000000

#define CHUNK 26214

namespace nabu {

const uint16_t currentVersion = 4;

std::string
CompressionString(CompressionType c)
{
    if (c == eZlibCompression)
        return "zlib compressed";
    else
        return "uncompressed";
}


DataFile::DataFile(const std::string& name) :
    mName(name),
    mVersion(0),
    mCount(0),
    mCompressionType(eZlibCompression),
    mUncompressedSize(0)
{
    LeakDetector::Get().Increment("DataFile");
}

DataFile::~DataFile()
{
    LeakDetector::Get().Decrement("DataFile");
}

const unsigned char magic[] = {0xed, 0xae, 0xfd, 0xaf};


/* File format
 *
 * MAGIC                         <4 bytes> 0xaeedaffd
 * VERSION                       <2 bytes> 0x00000003
 *
 * (version <= 3)
 *    earliest record : UTC SECONDS + NANOSECONDS    <4 bytes + 4 bytes>
 *    latest record   : UTC SECONDS + NANOSECONDS    <4 bytes + 4 bytes>
 * (version > 3)
 *    earliest record : UTC SECONDS + FEMPTOSECONDS  <8 bytes + 8 bytes>
 *    latest record   : UTC SECONDS + NANOSECONDS    <8 bytes + 8 bytes>
 *
 * NUMBER OF RECORDS IN FILE     <4 bytes>
 *
 * (version >= 3)
 *     compression type : <2 bytes>
 *                        0 -- none
 *                        1 -- zlib
 *     compressed bytes : <4 bytes>
 *        number of record bytes compressed (uncompressed size of records)
 *
 * (version >= 2)
 *     MD5 of header (everything after VERSION up to here)
 *
 * RECORD x NUMBER OF RECORDS IN FILE, possibly compressed
 *
 * MD5 OF ALL UNCOMPRESSED RECORD BYTES (itself not compressed)    <16 bytes>
 *
 * Record Format
 * (version <= 3)
 *    UTC SECONDS + NANOSECONDS     <4 bytes + 4 bytes>
 *    UTC SECONDS + NANOSECONDS     <4 bytes + 4 bytes>
 *    (TRR records only) ENDING UTC SECONDS + NANOSECONDS     <4 bytes + 4 bytes>
 *    (Region records only) demarcation type <4 bytes>
 *    (Searchable records only) JSON size <4 bytes>
 *
 * (version > 3)
 *    UTC SECONDS + ATTOSECONDS   <8 bytes + 8 bytes>
 *    ENDING UTC SECONDS + ATTOSECONDS   <8 bytes + 8 bytes>
 *    demarcation type <4 bytes>
 *    JSON size <4 bytes>
 *    DATA SIZE                   <4 bytes>
 *    JSON
 *    DATA
 *
 */

void
DataFile::ReadHeaderOnly()
{
    MD5_CTX md5Ctx;
    MD5_Init(&md5Ctx);

    cor::File file(mName, O_RDONLY);
    ReadHeaderOnly(file, &md5Ctx);
}

void
DataFile::ReadHeaderOnly(cor::File& file, void* md5CtxPtr)
{
    GetProfiler().Count("DataFile::ReadHeaderOnly");

    MD5_CTX* md5Ctx = (MD5_CTX*)md5CtxPtr;

    unsigned char buffer[100];
    size_t n = file.Read((char*)buffer, 4);
    if ((n != 4) || (strncmp((char*)buffer, (char*)magic, sizeof(magic)) != 0))
        throw cor::Exception("Invalid MAGIC");

    n = file.Read((char*)buffer, 2);
    if ((n != 2))
        throw cor::Exception("EOF encountered reading VERSION");
    unsigned int version = cor::Unpack16(buffer);
    if (version > currentVersion)
        throw cor::Exception("Unsupported data file version %d", version);

    mVersion = version;
    //printf("Version = %d\n", mVersion);

    // versions 2 and greater have the first and final time in the header
    // for fast extent fetching
    if (mVersion >= 2)
    {
        // versions 4 and higher use 64-bit time instead of 32-bit
        if (mVersion > 3)
        {
            n = file.Read((char*)buffer, 32);
            if (n != 32)
                throw cor::Exception("EOF encountered reading time range");

            int64_t sec = cor::Unpack64(buffer);
            int64_t fsec = cor::Unpack64(buffer + 8);
            mTimeRange.First() = cor::Time(sec, fsec);
            sec = cor::Unpack64(buffer + 16);
            fsec = cor::Unpack64(buffer + 24);
            mTimeRange.Final() = cor::Time(sec, fsec);

            //printf("mTimeRange = %s\n", mTimeRange.Print().c_str());

            MD5_Update(md5Ctx, buffer, 32);
        }
        else
        {
            n = file.Read((char*)buffer, 16);
            if (n != 16)
                throw cor::Exception("EOF encountered reading time range");

            uint32_t sec = cor::Unpack32(buffer);
            uint32_t nsec = cor::Unpack32(buffer + 4);
            mTimeRange.First() = cor::Time(sec, nsec);
            sec = cor::Unpack32(buffer + 8);
            nsec = cor::Unpack32(buffer + 12);
            mTimeRange.Final() = cor::Time(sec, nsec);

            MD5_Update(md5Ctx, buffer, 16);
        }
    }

    n = file.Read((char*)buffer, 4);
    if (n != 4)
        throw cor::Exception("EOF encountered reading NUM RECORDS");
    size_t numRecords = cor::Unpack32(buffer);
    //printf("numRecords = %ld\n", numRecords);
    MD5_Update(md5Ctx, buffer, 4);

    // versions 3 and higher have compression information
    if (mVersion >= 3)
    {
        n = file.Read((char*)buffer, 6);
        if (n != 6)
            throw cor::Exception("EOF encountered reading compression information");
        mCompressionType = (CompressionType)cor::Unpack16(buffer);
        mUncompressedSize = cor::Unpack32(buffer + 2);
        //printf("compression = %d, uncompressed size = %ld\n", (int)mCompressionType, mUncompressedSize);
        MD5_Update(md5Ctx, buffer, 6);
    }
    else
    {
        mCompressionType = eNoCompression;
    }

    // versions 2 and greater have a checksum for the 'header', which is
    // everything past version and up to here
    if (mVersion >= 2)
    {
        n = file.Read((char*)buffer, 16);
        if (n != 16)
            throw cor::Exception("EOF encountered reading header checksum");

        unsigned char md[MD5_DIGEST_LENGTH];
        MD5_Final(md, md5Ctx);
        for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
        {
            if (md[i] != buffer[i])
                throw cor::Exception("Header checksum error");
        }
    }

    mCount = numRecords;

    DEBUG_LOCAL("DataFile::ReadHeader from %s complete\n",  mName.c_str());
    DEBUG_LOCAL("version           : %d\n", mVersion);
    DEBUG_LOCAL("compression       : %s\n", CompressionString(mCompressionType).c_str());
    DEBUG_LOCAL("time range        : %s\n", mTimeRange.Print().c_str());
    DEBUG_LOCAL("count             : %ld\n", mCount);
    DEBUG_LOCAL("uncompressed size : %ld\n", mUncompressedSize);
}

bool
DataFile::CheckIntegrity(std::string& problem)
{
    try
    {
        // compute md5
        MD5_CTX md5Ctx;
        MD5_Init(&md5Ctx);

        cor::File file(mName, O_RDONLY);
        ReadHeaderOnly(file, &md5Ctx);

        size_t currentOffset = file.Where();
        int count = file.Size() - MD5_DIGEST_LENGTH - currentOffset;

        if (mCompressionType == eNoCompression)
        {
            char buffer[1000];
            while (count > 0)
            {
                size_t grab = count < sizeof(buffer) ? count : sizeof(buffer);
                size_t n = file.Read(buffer, grab);
                if (n != grab)
                    throw cor::Exception("Unexpected EOF checking integrity of %s", mName.c_str());
                MD5_Update(&md5Ctx, buffer, n);
                count -= n;
            }

            // now read MD5
            size_t n = file.Read(buffer, MD5_DIGEST_LENGTH);
            if (n != MD5_DIGEST_LENGTH)
                throw cor::Exception("Unexpected EOF checking integrity of %s at MD5", mName.c_str());

            unsigned char md[MD5_DIGEST_LENGTH];
            MD5_Final(md, &md5Ctx);
            for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
            {
                if (md[i] != (unsigned char)buffer[i])
                {
                    //printf("    Read CS %s", cor::HexDump(buffer, MD5_DIGEST_LENGTH).c_str());
                    //printf("Computed CS %s", cor::HexDump(md, MD5_DIGEST_LENGTH).c_str());
                    throw cor::Exception("Checksum error");
                }
            }

            return true;
        }
        else // compressed
        {

            z_stream strm;
            // allocate inflate state
            strm.zalloc = Z_NULL;
            strm.zfree = Z_NULL;
            strm.opaque = Z_NULL;
            strm.avail_in = 0;
            strm.next_in = Z_NULL;
            int ret = inflateInit(&strm);
            if (ret != Z_OK)
                throw cor::Exception("Error initializing decompression");

            try
            {
                unsigned char in[CHUNK];
                unsigned char out[CHUNK];

                unsigned char buffer[100];


                // decompress until deflate stream ends or end of file
                do {
                    size_t n = file.Read((char*)in, CHUNK);
                    if (n == 0)
                        break;
                    //printf("Read: %ld available\n", n);
                    strm.next_in = in;
                    strm.avail_in = n;

                    // run inflate() on input until output buffer not full
                    do {
                        strm.avail_out = CHUNK;
                        strm.next_out = out;
                        ret = inflate(&strm, Z_NO_FLUSH);
                        unsigned char* parsePtr = out;

                        switch (ret) {
                            case Z_NEED_DICT:
                                throw cor::Exception("Need dictionary for decompression");
                            case Z_STREAM_END:
                                //printf("End of inflate stream detected\n");
                                break;
                            case Z_DATA_ERROR:
                                throw cor::Exception("Data error during decompression");
                            case Z_MEM_ERROR:
                                throw cor::Exception("Memory error during decompression");
                        }
                        size_t have = CHUNK - strm.avail_out;

                        if (have != 0)
                            MD5_Update(&md5Ctx, out, have);

                    } while (strm.avail_out == 0); // output buffer not full

                    // done when inflate() says it's done
                } while (ret != Z_STREAM_END);

                // the final 16 bytes in the file, the MD5, might be partially or
                // completely in the inflate buffer; grab whatever remains there as
                // MD5, and read any remainder directly from the file
                //printf("%ld bytes remain, copying them to CRC\n", strm.avail_in);
                memcpy(buffer, strm.next_in, strm.avail_in);
                //printf("%s\n", cor::HexDump(buffer, strm.avail_in).c_str());
                size_t n = file.Read((char*)buffer + strm.avail_in, MD5_DIGEST_LENGTH - strm.avail_in);
                //printf("Read %ld CRC bytes\n", n);

                if (n + strm.avail_in != MD5_DIGEST_LENGTH)
                    throw cor::Exception("EOF encountered reading CRC");

                unsigned char md[MD5_DIGEST_LENGTH];
                MD5_Final(md, &md5Ctx);
                for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
                {
                    if (md[i] != buffer[i])
                    {
                        throw cor::Exception("Checksum error: \n  stored %s\ncomputed %s",
                                             cor::HexDump(buffer, MD5_DIGEST_LENGTH).c_str(),
                                             cor::HexDump(md, MD5_DIGEST_LENGTH).c_str());
                    }
                }

                inflateEnd(&strm);

                return true;
            }
            catch (...)
            {
                inflateEnd(&strm);
                throw;
            }
        }
    } catch (const cor::Exception& err)
    {
        problem = err.what();
    }

    return false;
}

// compute the size of the per-record header based on column info
size_t
RecordHeaderSize(int version)
{
    // version 4 and higher; 44 bytes
    return 20 + // 16 bytes for time, and 4 bytes for size
           4  + // 4 bytes for JSON size
           16 + // 16 bytes for ending timestamp
           4 ;  // 4 bytes for demarcation type (obsolete, but kept)
}



void
DataFile::Read(RecordVector& records, IterationDirection direction)
{
    GetProfiler().Count("DataFile::Read");

    // compute md5
    MD5_CTX md5Ctx;
    MD5_Init(&md5Ctx);

    cor::File file(mName, O_RDONLY);
    ReadHeaderOnly(file, &md5Ctx);

    // increase the size of records to hold the data in this file
    records.resize(records.size() + mCount);

    try
    {
        if (mCompressionType == eNoCompression)
        {
            ReadUncompressedRecords(file, md5Ctx, records, direction);
        }
        else // compressed
        {
            ReadCompressedRecords(file, md5Ctx, records, direction);
        }

    } catch (cor::Exception& err)
    {
        err.SetWhat("DataFile::Read() -- %s : %s", Name().c_str(), err.what());
        throw err;
    } catch (const std::exception& err)
    {
        cor::Exception out;
        out.SetWhat("DataFile::Read() -- %s : %s", Name().c_str(), err.what());
        throw out;
    }

    DEBUG_LOCAL("DataFile::Read of %ld records from %s complete\n", records.size(), mName.c_str());
}

void
DataFile::ReadCompressedRecords(cor::File &file,
                         MD5_CTX &md5Ctx,
                         nabu::RecordVector &records,
                         IterationDirection direction)
{
    DEBUG_LOCAL("DataFile::ReadCompressed() -- Reading %ld records\n", mCount);

    if (mCount == 0)
        return;

    z_stream strm;
    // allocate inflate state
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    int ret = inflateInit(&strm);
    if (ret != Z_OK)
        throw cor::Exception("Error initializing uncompression");

    unsigned char in[CHUNK];
    unsigned char out[CHUNK];

    unsigned char buffer[100];
    size_t nRead = 0; // number of records read from this file

    try
    {
        const size_t recordHeaderSize = RecordHeaderSize(mVersion);

        // first data is intended for the header of the first record
        size_t headerRemainder = recordHeaderSize;
        size_t dataRemainder = 0;
        size_t jsonRemainder = 0;
        // NOTE: this is set during header parsing
        size_t jsonSize = 0;

        char jsonBuffer[CHUNK];


        Record record;
        unsigned char headerBuffer[recordHeaderSize];

        // decompress until deflate stream ends or end of file
        do {
            size_t n = file.Read((char*)in, CHUNK);
            if (n == 0)
                break;
            //printf("Read: %ld available\n", n);
            strm.next_in = in;
            strm.avail_in = n;

            // run inflate() on input until output buffer not full
            do {
                strm.avail_out = CHUNK;
                strm.next_out = out;
                ret = inflate(&strm, Z_NO_FLUSH);
                unsigned char* parsePtr = out;


                switch (ret) {
                    case Z_NEED_DICT:
                        throw cor::Exception("Need dictionary for decompression");
                    case Z_STREAM_END:
                        //printf("End of inflate stream detected: where = %ld\n", file.Where());
                        //printf("current records = %ld\n", nRead);
                        break;
                    case Z_DATA_ERROR:
                        throw cor::Exception("Data error during decompression");
                    case Z_MEM_ERROR:
                        throw cor::Exception("Memory error during decompression");
                }
                size_t have = CHUNK - strm.avail_out;
                //printf("Read: have = %ld\n", have);

                while (have != 0)
                {
                    // searchable JSON
                    if (jsonRemainder != 0)
                    {
                        //printf("Parsing JSON (remainder = %ld)\n", jsonRemainder);
                        size_t cnt = have > jsonRemainder ? jsonRemainder : have;
                        //printf("cnt = %ld\n", cnt);
                        memcpy(jsonBuffer + (jsonSize - jsonRemainder), parsePtr, cnt);
                        jsonRemainder -= cnt;
                        have -= cnt;
                        parsePtr += cnt;

                        if (jsonRemainder == 0)
                        {
                            MD5_Update(&md5Ctx, jsonBuffer, jsonSize);

                            Json::Reader reader;
                            if (!reader.parse(jsonBuffer, jsonBuffer + jsonSize, record.mDataObject))
                                throw cor::Exception("Error parsing JSON in record %ld: %s\n",
                                                     nRead,
                                                     reader.getFormattedErrorMessages().c_str());

                            // prepare for reading data
                            dataRemainder = record.mData->size();

                            // handle the case of a record with json, but no data
                            if (dataRemainder == 0)
                            {
                                // commit this record and arm for the next header if not done
                                if (direction == eIterateForwards)
                                    records[nRead++] = record;
                                else
                                    records[records.size() - nRead++ - 1] = record;
                                if (nRead < mCount)
                                {
                                    headerRemainder = recordHeaderSize;
                                }
                            }
                        }
                    }

                    // data
                    if (dataRemainder != 0)
                    {
                        //printf("Parsing data (remainder = %ld)\n", dataRemainder);
                        size_t cnt = have > dataRemainder ? dataRemainder : have;
                        //printf("cnt = %ld\n", cnt);
                        memcpy(record.mData->data() + (record.mData->size() - dataRemainder), parsePtr, cnt);
                        dataRemainder -= cnt;
                        have -= cnt;
                        parsePtr += cnt;

                        // if this completes the data record, commit it and possibly prepare
                        // for the next header
                        if (dataRemainder == 0)
                        {
                            MD5_Update(&md5Ctx, record.mData->data(), record.mData->size());

                            /*if (columnInfo.mSearchable)
                            {
                                Json::Reader reader;
                                if (!reader.parse((const char*)record.mData->data(),
                                                  (const char*)record.mData->data() + jsonSize,
                                                  record.mDataObject))
                                {
                                    throw cor::Exception("Error parsing searchable JSON in record %ld: %s",
                                        reader.getFormattedErrorMessages().c_str(),
                                        nRead);
                                }
                            }*/
                            if (direction == eIterateForwards)
                                records[nRead++] = record;
                            else
                                records[records.size() - nRead++ - 1] = record;

                            if (nRead < mCount)
                            {
                                headerRemainder = recordHeaderSize;
                            }
                            else if (have != 0)
                            {
                                // there are no more records left, but there
                                // is data to consume; something is wrong
                                throw cor::Exception("Corrupted or mis-typed data file");
                            }
                        }
                    }

                    // header
                    if (headerRemainder != 0)
                    {
                        //printf("Parsing header (remainder = %ld)\n", headerRemainder);
                        size_t cnt = have > headerRemainder ? headerRemainder : have;
                        //printf("cnt = %ld\n", cnt);
                        memcpy(headerBuffer + recordHeaderSize - headerRemainder, parsePtr, cnt);
                        headerRemainder -= cnt;
                        have -= cnt;
                        parsePtr += cnt;

                        // if that is a complete header, parse it and prepare for the
                        // data that follows
                        if (headerRemainder == 0)
                        {
                            const unsigned char* headerPtr = headerBuffer;
                            // parse the header and set up the Record
                            if (mVersion <= 3)
                            {
                                throw cor::Exception("Obsolete data format encountered");
                                /*struct timespec ts;
                                ts.tv_sec = cor::Unpack32(&headerPtr);
                                ts.tv_nsec = cor::Unpack32(&headerPtr);
                                if (columnInfo.mRecordType == eTRR)
                                {
                                    struct timespec endTs;
                                    endTs.tv_sec = cor::Unpack32(&headerPtr);
                                    endTs.tv_nsec = cor::Unpack32(&headerPtr);
                                    record.SetTimeRange(cor::TimeRange(ts, endTs));
                                    //printf("Read time range %s\n", record.mTimeRange.Print().c_str());
                                }
                                else
                                {
                                    record.SetTime(ts);
                                    //printf("Read time %s\n", record.GetTime().Print().c_str());
                                }*/
                            }
                            else
                            {
                                BigTime bt1;
                                bt1.mSeconds = cor::Unpack64(&headerPtr);
                                bt1.mAttoSeconds = cor::Unpack64(&headerPtr);
                                BigTime bt2;
                                bt2.mSeconds = cor::Unpack64(&headerPtr);
                                bt2.mAttoSeconds = cor::Unpack64(&headerPtr);

                                record.SetTimeRange(cor::TimeRange(bt1, bt2));
                            }

                            if (mVersion > 3)
                            {
                                cor::Unpack32(&headerPtr); // This was previously demarcation. Keeping it here for consistency with write code  documentation.
                                //printf("Read demarcation %d\n", record.mDemarcation);
                            }

                            /*if ((mVersion > 3) || columnInfo.mSearchable)
                            {
                                jsonSize = cor::Unpack32(&headerPtr);
                                //printf("Read jsonSize %ld\n", jsonSize);
                            }*/
                            jsonSize = cor::Unpack32(&headerPtr);

                            size_t recordSize = cor::Unpack32(&headerPtr);
                            MD5_Update(&md5Ctx, headerBuffer, recordHeaderSize);

                            // SANITY
                            //printf("jsonSize = %ld, limit = %u\n", jsonSize, JSON_SIZE_LIMIT);
                            if (jsonSize > JSON_SIZE_LIMIT)
                            {
                                std::string what;
                                if (!CheckIntegrity(what))
                                    throw cor::Exception("Corrupted file %s: %s", Name().c_str(), what.c_str());
                                throw cor::Exception("File %s is uncorrupted but has too large JSON size (%ld): "
                                                     "this may be from an incompatible version of the Nabu database",
                                                     Name().c_str(), jsonSize);
                            }
                            //printf("recordSize = %ld, limit = %u\n", recordSize, RECORD_SIZE_LIMIT);
                            if (recordSize > RECORD_SIZE_LIMIT)
                            {
                                std::string what;
                                if (!CheckIntegrity(what))
                                    throw cor::Exception("Corrupted file %s: %s", Name().c_str(), what.c_str());
                                throw cor::Exception("File %s is uncorrupted but has too large record count (%ld): "
                                                     "this may be from an incompatible version of the Nabu database",
                                                     Name().c_str(), recordSize);
                            }

                            record.mData.reset(new std::vector<unsigned char>);
                            record.mData->resize(recordSize);
                            /*printf("header: time = %s, jsonSize = %ld, recordSize = %ld\n",
                                   record.GetTime().Print().c_str(),
                                   jsonSize,
                                   recordSize);*/

                            // if there is searchable JSON, parse that next; else move
                            // on to data
                            if (jsonSize != 0)
                                jsonRemainder = jsonSize;
                            else
                                dataRemainder = recordSize;

                            // handle the case of a record with no json or data
                            if ((jsonRemainder == 0) && (dataRemainder == 0))
                            {
                                // commit this record and arm for the next header if not done
                                if (direction == eIterateForwards)
                                    records[nRead++] = record;
                                else
                                    records[records.size() - nRead++ - 1] = record;

                                if (nRead < mCount)
                                {
                                    headerRemainder = recordHeaderSize;
                                }
                                else if (have != 0)
                                {
                                    // there are no more records left, but there
                                    // is data to consume; something is wrong
                                    throw cor::Exception("Corrupted or mis-typed data file");
                                }
                            }
                        }
                    }
                } // end while have != 0

            } while (strm.avail_out == 0); // output buffer not full

            // done when inflate() says it's done
        } while (ret != Z_STREAM_END);


        // DEFENSIVE
        if (nRead != mCount)
            throw cor::Exception("Did not decompress the expected number of records; got %ld, expected %ld",
                                 nRead,
                                 mCount);

        // the final 16 bytes in the file, the MD5, might be partially or
        // completely in the inflate buffer; grab whatever remains there as
        // MD5, and read any remainder directly from the file
        //printf("%ld bytes remain, copying them to CRC\n", strm.avail_in);
        memcpy(buffer, strm.next_in, strm.avail_in);
        //printf("%s\n", cor::HexDump(buffer, strm.avail_in).c_str());
        size_t n = file.Read((char*)buffer + strm.avail_in, MD5_DIGEST_LENGTH - strm.avail_in);
        //printf("Read %ld CRC bytes\n", n);

        if (n + strm.avail_in != MD5_DIGEST_LENGTH)
        {
            printf("where = %ld\n", file.Where());
            throw cor::Exception("EOF encountered reading CRC: read = %ld, remainder = %ld, total expected = %ld",
                                 n,
                                 strm.avail_in,
                                 MD5_DIGEST_LENGTH);
        }

        unsigned char md[MD5_DIGEST_LENGTH];
        MD5_Final(md, &md5Ctx);
        for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
        {
            if (md[i] != buffer[i])
            {
                throw cor::Exception("Checksum error: \n  stored %s\ncomputed %s",
                                     cor::HexDump(buffer, MD5_DIGEST_LENGTH).c_str(),
                                     cor::HexDump(md, MD5_DIGEST_LENGTH).c_str());
            }
        }
    } catch (...)
    {
        inflateEnd(&strm);
        throw;
    }

    inflateEnd(&strm);
}

void
DataFile::ReadUncompressedRecords(cor::File& file,
                           MD5_CTX& md5Ctx,
                           nabu::RecordVector &records,
                           IterationDirection direction)
{
    unsigned char buffer[100];

    DEBUG_LOCAL("DataFile::ReadUncompressed() -- Reading %ld records\n", mCount);
    //printf("version = %d\n", mVersion);
    for (size_t i = 0; i < mCount; i++)
    {
        Record record;

        // time of record (ePIT) or beginning of record (eTRR)
        cor::Time begin;
        if (mVersion <= 3)
        {
            size_t n = file.Read((char*)buffer, 8);
            if (n != 8)
                throw cor::Exception("EOF encountered reading timestamp in record %d", i);
            
            struct timespec ts;
            ts.tv_sec = cor::Unpack32(buffer);
            ts.tv_nsec = cor::Unpack32(buffer + 4);
            begin = ts;
            MD5_Update(&md5Ctx, buffer, 8);
        }
        else
        {
            size_t n = file.Read((char*)buffer, 16);
            if (n != 16)
                throw cor::Exception("EOF encountered reading timestamp in record %d", i);
            
            BigTime bt;
            bt.mSeconds = cor::Unpack64(buffer);
            bt.mAttoSeconds = cor::Unpack64(buffer + 8);
            MD5_Update(&md5Ctx, buffer, 16);
            begin = cor::Time(bt);
        }
        //printf("begin = %s\n", begin.Print().c_str());
        
        // ending timestamp
        if (mVersion <= 3)
            throw cor::Exception("Obsolete data file format");

        //if ((mVersion > 3) || columnInfo.mRecordType == eTRR)
        {
            cor::Time end;
            if (mVersion <= 3)
            {
                size_t n = file.Read((char*)buffer, 8);
                if (n != 8)
                    throw cor::Exception("EOF encountered reading timestamp in record %d", i);
                
                struct timespec endTs;
                endTs.tv_sec = cor::Unpack32(buffer);
                endTs.tv_nsec = cor::Unpack32(buffer + 4);
                end = endTs;
                MD5_Update(&md5Ctx, buffer, 8);
            }
            else
            {
                size_t n = file.Read((char*)buffer, 16);
                if (n != 16)
                    throw cor::Exception("EOF encountered reading timestamp in record %d", i);
                
                BigTime bt;
                bt.mSeconds = cor::Unpack64(buffer);
                bt.mAttoSeconds = cor::Unpack64(buffer + 8);
                end = bt;
                MD5_Update(&md5Ctx, buffer, 16);
                //printf("end = %s\n", end.Print().c_str());
            }

            record.SetTimeRange(cor::TimeRange(begin, end));
        }
        //else
       // {
        //    record.SetTime(begin);
       // }
        DEBUG_LOCAL("DataFile::ReadUncompressed() -- record %ld: %s\n", i, record.mTimeRange.Print().c_str());

        //if (mVersion > 3)
        {
            size_t n = file.Read((char*)buffer, 4);
            if (n != 4)
                throw cor::Exception("EOF encountered reading demarcation type for record %d", i);
            // this used to be 'demarcation' for Regions, no longer supported
            MD5_Update(&md5Ctx, buffer, 4);
        }

        // searchable JSON
        size_t jsonSize = 0;
        //if ((mVersion > 3) || columnInfo.mSearchable)
        {
            // size of JSON
            size_t n = file.Read((char*)buffer, 4);
            if (n != 4)
                throw cor::Exception("EOF encountered reading JSON size for record %ld", i);
            jsonSize = cor::Unpack32(buffer);
            MD5_Update(&md5Ctx, buffer, 4);
        }
        DEBUG_LOCAL("DataFile::ReadUncompressed() -- record %ld: jsonSize %ld\n", i, jsonSize);

        // size of record
        size_t n = file.Read((char*)buffer, 4);
        if (n != 4)
            throw cor::Exception("EOF encountered reading size for record %ld", i);
        size_t recordSize = cor::Unpack32(buffer);
        MD5_Update(&md5Ctx, buffer, 4);
        DEBUG_LOCAL("DataFile::ReadUncompressed() -- record %ld: recordSize %ld\n", i, recordSize);

        // SANITY
        if (jsonSize > JSON_SIZE_LIMIT)
        {
            std::string what;
            if (!CheckIntegrity(what))
                throw cor::Exception("Corrupted file %s: %s", Name().c_str(), what.c_str());
            throw cor::Exception("File %s is uncorrupted but has too large JSON size (%ld): "
                                 "this may be from an incompatible version of the Nabu database",
                                 Name().c_str(), jsonSize);
        }
        if (recordSize > RECORD_SIZE_LIMIT)
        {
            std::string what;
            if (!CheckIntegrity(what))
                throw cor::Exception("Corrupted file %s: %s", Name().c_str(), what.c_str());
            throw cor::Exception("File %s is uncorrupted but has too large record count (%ld): "
                                 "this may be from an incompatible version of the Nabu database",
                                 Name().c_str(), recordSize);
        }

        // JSON payload
        if (jsonSize != 0)
        {
            char jsonBuffer[jsonSize];
            n = file.Read(jsonBuffer, jsonSize);
            if (n != jsonSize)
                throw cor::Exception("EOF encountered reading JSON record %ld", i);

            // parse JSON
            Json::Reader reader;
            if (!reader.parse(std::string(jsonBuffer, jsonSize), record.mDataObject))
                throw cor::Exception("Bad JSON in searchable record %ld: %s", i, reader.getFormattedErrorMessages().c_str());

            MD5_Update(&md5Ctx, jsonBuffer, jsonSize);
        }

        record.mData.reset(new std::vector<unsigned char>);
        record.mData->resize(recordSize);

        n = file.Read((char*)record.mData->data(), recordSize);
        if (n != recordSize)
            throw cor::Exception("EOF encountered reading data (size %ld) for record %d", recordSize, i);
        MD5_Update(&md5Ctx, record.mData->data(), recordSize);

        if (direction == eIterateForwards)
            records[i] = record;
        else
            records[records.size() - i - 1] = record;
    }

    DEBUG_LOCAL("DataFile::ReadUncompressed() -- Reading checksum%s\n","");
    size_t n = file.Read((char*)buffer, 16);
    if (n != 16)
        throw cor::Exception("EOF encountered reading checksum");

    unsigned char md[MD5_DIGEST_LENGTH];
    MD5_Final(md, &md5Ctx);
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
    {
        if (md[i] != buffer[i])
            throw cor::Exception("Checksum error");
    }
}

size_t
CompressStreamToFile(cor::File& file, z_stream& strm, unsigned char* out, bool finish)
{
    DEBUG_LOCAL("Compressing %d bytes to file %s\n", strm.avail_in, file.GetName().c_str());
    size_t wrote = 0;
    do {
        strm.avail_out = CHUNK;
        strm.next_out = out;
        int ret = deflate(&strm, finish ? Z_FINISH : Z_NO_FLUSH);
        // DEFENSIVE; should only happen if stream state is clobbered by someone
        if (ret == Z_STREAM_ERROR)
            throw cor::Exception("Error compressing data file: %d\n", ret);

        size_t have = CHUNK - strm.avail_out;
        wrote += have;
        //printf("Write: have = %ld\n", have);
        file.WriteAll((char*)out, have);

    } while (strm.avail_out == 0);

    //printf("   Wrote %ld compressed bytes\n", wrote);

    return wrote;
}

void
DataFile::Write(const RecordVector& records,
    IterationDirection direction,
    CompressionType compress
    )
{
    DEBUG_LOCAL("DataFile::Write %s %ld records, %s\n",
                mName.c_str(),
                records.size(),
                CompressionString(compress).c_str());

    mVersion = currentVersion;

    cor::TimeRange tr;
    if (!records.empty())
    {
        tr = cor::TimeRange(records.front().mTimeRange.First(), records.back().mTimeRange.Final());
    }
    Context ctx("DataFile::Write %d records, time range %s", records.size(), tr.Print().c_str());

    try
    {
        mCompressionType = compress;

        GetProfiler().Count("DataFile::Write");
        cor::File::MakePath(mName);
        cor::File file(mName, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IROTH);

        size_t n = file.Write((char*)magic, 4);
        if (n != 4)
            throw cor::Exception("EOF writing magic");

        unsigned char buffer[100];
        cor::Pack16(buffer, currentVersion);
        n = file.Write((char*)buffer, 2);
        if ((n != 2))
            throw cor::Exception("EOF writing VERSION");

        // compute md5
        MD5_CTX md5Ctx;
        MD5_Init(&md5Ctx);

        // time ranges in file
        cor::TimeRange tr;
        if (records.empty())  // DEFENSIVE
        {
            printf("!!!! Writing data file with no data in it\n\n");

            // make first > final, which is Invalid
            tr.First() = cor::Time(1);
            tr.Final() = cor::Time(0);
        }
        else
        {
            tr = cor::TimeRange(records.front().mTimeRange.First(), records.back().mTimeRange.Final());
        }

        cor::Pack64(buffer, tr.First().SecondPortion());
        cor::Pack64(buffer + 8, tr.First().AttoSecondPortion());
        cor::Pack64(buffer + 16, tr.Final().SecondPortion());
        cor::Pack64(buffer + 24, tr.Final().AttoSecondPortion());
        MD5_Update(&md5Ctx, buffer, 32);
        n = file.Write((char*)buffer, 32);
        if (n != 32)
            throw cor::Exception("EOF writing header timerange");

        cor::Pack32(buffer, records.size());
        n = file.Write((char*)buffer, 4);
        if (n != 4)
            throw cor::Exception("EOF writing NUM RECORDS");
        MD5_Update(&md5Ctx, buffer, 4);

        const size_t recordHeaderSize = RecordHeaderSize(mVersion);

        // compression info
        cor::Pack16(buffer, mCompressionType);
        // total compressed size is:
        //   total data size
        //   recordHeaderSize per record
        mUncompressedSize = records.TotalSize() + (recordHeaderSize * records.size());
        cor::Pack32(buffer + 2, mUncompressedSize);
        n = file.Write((char*)buffer, 6);
        if (n != 6)
            throw cor::Exception("EOF writing compression info");
        MD5_Update(&md5Ctx, buffer, 6);

        // md5 up to this point (the header)
        {
            unsigned char md[MD5_DIGEST_LENGTH];
            MD5_Final(md, &md5Ctx);
            n = file.Write((char*)md, MD5_DIGEST_LENGTH);
            //printf("header MD5: %s\n", cor::HexDump(md, MD5_DIGEST_LENGTH).c_str());
            if (n != MD5_DIGEST_LENGTH)
                throw cor::Exception("EOF writing header checksum");
        }

        size_t compressedSize = 0;

        if (mCompressionType == eZlibCompression)
        {
            WriteCompressedRecords(file, md5Ctx, records, direction);
        }
        else
        {
            WriteUncompressedRecords(file, md5Ctx, records, direction);
        }

        unsigned char md[MD5_DIGEST_LENGTH];
        MD5_Final(md, &md5Ctx);
        n = file.Write((char*)md, MD5_DIGEST_LENGTH);
        //printf("File MD5: %s\n", cor::HexDump(md, MD5_DIGEST_LENGTH).c_str());
        if (n != MD5_DIGEST_LENGTH)
            throw cor::Exception("EOF writing file checksum");

    } catch (cor::Exception& err)
    {
        err.SetWhat("DataFile::Write() -- %s : %s", Name().c_str(), err.what());
        throw err;
    } catch (const std::exception& err)
    {
        cor::Exception out;
        out.SetWhat("DataFile::Write() -- %s : %s", Name().c_str(), err.what());
        throw out;
    }

    DEBUG_LOCAL("DataFile::Write of %ld records to %s complete\n", records.size(), mName.c_str());
}

void
DataFile::WriteCompressedRecords(cor::File &file,
                          MD5_CTX &md5Ctx,
                          const nabu::RecordVector &records,
                          IterationDirection direction)
{
    z_stream strm;
    unsigned char in[CHUNK];
    unsigned char out[CHUNK];

    unsigned char buffer[100];

    size_t compressedSize = 0;

    const size_t recordHeaderSize = RecordHeaderSize(mVersion);

    try
    {
        // allocate deflate state
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        int r = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
        if (r != Z_OK)
        {
            throw cor::Exception("Error initializing compression: %d\n", r);
        }

        cor::Time t(cor::Time::eInvalid); // detect non-increasing time entries

        for (size_t i = 0; i < records.size(); i++)
        {
            const Record& r = direction == eIterateForwards ? records[i] : records[records.size() - 1 - i];

            // DEFENSIVE -- detect backwards slip in time
            if (t.Valid() && (r.mTimeRange.First() <= t))
            {
                throw FatalException(cor::Url(), // no specific database
                                     "non-increasing time %s detected after %s",
                                     r.PrintTime().c_str(),
                                     t.Print().c_str());
            }
            t = r.mTimeRange.Final();


            strm.avail_in = recordHeaderSize;
            strm.next_in = buffer;

            unsigned char* bufPtr = buffer;

            cor::Pack64(&bufPtr, r.mTimeRange.First().SecondPortion());
            cor::Pack64(&bufPtr, r.mTimeRange.First().AttoSecondPortion());
            cor::Pack64(&bufPtr, r.mTimeRange.Final().SecondPortion());
            cor::Pack64(&bufPtr, r.mTimeRange.Final().AttoSecondPortion());
            cor::Pack32(&bufPtr, 0xdeadbeef); // previously 'demarcation'
            
            std::string js;
            {
                //printf("Writing searchable\n");
                Json::FastWriter writer;
                js = writer.write(r.mDataObject);

                // JSON size
                cor::Pack32(&bufPtr, js.size());
            }

            //printf("Writing data size: %d\n", r.mData->size());
            cor::Pack32(&bufPtr, r.mData->size());

            // write the compressed header
            compressedSize += CompressStreamToFile(file, strm, out, false);
            MD5_Update(&md5Ctx, buffer, recordHeaderSize);

            // write the JSON
            {
                strm.avail_in = js.size();
                strm.next_in = (unsigned char*)js.data();
                compressedSize += CompressStreamToFile(file, strm, out, false);
                MD5_Update(&md5Ctx, js.data(), js.size());
            }

            // DEFENSIVE
            if (strm.avail_in != 0)
                throw cor::Exception("1: Not all input used at end of writing compressed data stream");

            strm.next_in = r.mData->data();
            strm.avail_in = r.mData->size();
            MD5_Update(&md5Ctx, r.mData->data(), r.mData->size());

            // run deflate() on input until output buffer not full, finish
            // compression if all of source has been read in
            compressedSize += CompressStreamToFile(file, strm, out, i == (records.size() - 1));

            // DEFENSIVE
            if (strm.avail_in != 0)
                throw cor::Exception("2: Not all input used at end of writing compressed data stream");

        } // end for all records

        //printf("Finished compression: %ld -> %ld\n", mUncompressedSize, compressedSize);

    } catch (...)
    {
        deflateEnd(&strm);
        throw;
    }

    deflateEnd(&strm);
}

void
DataFile::WriteUncompressedRecords(cor::File &file,
                            MD5_CTX &md5Ctx,
                            const nabu::RecordVector &records,
                            IterationDirection direction)
{

    unsigned char buffer[100];

    for (size_t i = 0; i < records.size(); i++)
    {
        const Record& r = direction == eIterateForwards ? records[i] : records[records.size() - 1 - i];

        cor::Pack64(buffer, r.mTimeRange.First().SecondPortion());
        cor::Pack64(buffer + 8, r.mTimeRange.First().AttoSecondPortion());
        cor::Pack64(buffer + 16, r.mTimeRange.Final().SecondPortion());
        cor::Pack64(buffer + 24, r.mTimeRange.Final().AttoSecondPortion());
        size_t n = file.Write((char*)buffer, 32);
        MD5_Update(&md5Ctx, buffer, 32);
        if (n != 32)
            throw cor::Exception("EOF writing ending timestamp in record %ld", i);
        cor::Pack32(buffer, 0xdeadbeef); // previously 'demarcation'
        n = file.Write((char*)buffer, 4);
        if (n != 4)
            throw cor::Exception("EOF writing demarcation type in record %ld", i);
        MD5_Update(&md5Ctx, buffer, 4);


        std::string js;
        {
            Json::FastWriter writer;
            js = writer.write(r.mDataObject);

            // JSON size
            cor::Pack32(buffer, js.size());
            size_t n = file.Write((char*)buffer, 4);
            if (n != 4)
                throw cor::Exception("EOF writing searchable JSON size for record %ld", i);
            MD5_Update(&md5Ctx, buffer, 4);
        }

        cor::Pack32(buffer, r.mData->size());
        n = file.Write((char*)buffer, 4);
        if (n != 4)
            throw cor::Exception("EOF writing size for record %ld", i);
        MD5_Update(&md5Ctx, buffer, 4);

        {
            // JSON
            file.WriteAll(js.data(), js.size());
            MD5_Update(&md5Ctx, js.data(), js.size());
        }

        n = file.Write((char*)r.mData->data(), r.mData->size());
        if (n != records[i].mData->size())
            throw cor::Exception("EOF writing data for record %ld", i);
        MD5_Update(&md5Ctx, r.mData->data(), r.mData->size());
    }
}

bool
DataFile::Exists() const
{
    return cor::File::Exists(mName);
}
    
}
