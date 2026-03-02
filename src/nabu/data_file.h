//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_DATA_FILE__
#define __PKG_NABU_DATA_FILE__

#include <memory>
#include <vector>

#include <openssl/md5.h>

#include <json/json.h>
#include <mcor/mfile.h>
#include <mcor/timerange.h>

#include "interface_types.h"
#include "record.h"

namespace nabu
{

std::string CompressionString(CompressionType c);

/** class DataFile : the file that holds data records
 *
 *  A note about 'direction'. This flag on the Read and Write methods controls the order
 *  of data in the RecordVector only. The data in the file is always in 'forward'
 *  order, i.e., index 0 has the earliest time and each subsequent record is increasing
 *  time. If the 'backwards' flag is set, then the data will be delivered in the RecordVector
 *  in the opposite of this order during Read. During a Write, the data will be expected to
 *  be in this same opposite order. Thus data files can be used in either order, but in
 *  general, if an operation reads data in reverse order, it needs to write the data in
 *  reverse order, or expect a runtime exception to be encountered due to out-of-order
 *  data.
 *
 */

class DataFile
{
public:
    DataFile(const std::string& fileName);
    ~DataFile();

    void ReadHeaderOnly();
    bool CheckIntegrity(std::string& problem);
    void Read(RecordVector& records, IterationDirection direction);
    void Write(const RecordVector& records, IterationDirection direction, CompressionType compress = eZlibCompression);

    // does this file exist
    bool Exists() const;

    // return version of this file format
    uint16_t Version() const { return mVersion; }
    // return time range of records held
    const cor::TimeRange& TimeRange() const { return mTimeRange; }
    // return number of records held
    size_t Size() const { return mCount; }
    // return compression type
    CompressionType GetCompressionType() const { return mCompressionType; }
    std::string GetCompressionString() const { return CompressionString(mCompressionType); }
    // return uncompressed size
    size_t GetUncompressedSize() const { return mUncompressedSize; }

    const std::string& Name() const { return mName; }

protected:
    const std::string mName;

    // read the header, and only the header, for speed
    // md5 context passed as void* to hide the type in this header file
    void ReadHeaderOnly(cor::File& file, void* md5Ctx);

    void ReadCompressedRecords(cor::File& file,
                        MD5_CTX& md5Ctx,
                        RecordVector& records,
                        IterationDirection direction);
    void ReadUncompressedRecords(cor::File& file,
                          MD5_CTX& md5Ctx,
                          RecordVector& records,
                          IterationDirection direction);

    void WriteCompressedRecords(cor::File& file,
                         MD5_CTX& md5Ctx,
                         const RecordVector& records,
                         IterationDirection direction);
    void WriteUncompressedRecords(cor::File& file,
                           MD5_CTX& md5Ctx,
                           const RecordVector& records,
                           IterationDirection direction);

    // ==== information from header

    // version of this file
    uint16_t mVersion;
    // time range of Records held in this file
    cor::TimeRange mTimeRange;
    // number of records held in this file
    size_t mCount;

    CompressionType mCompressionType;
    // number of bytes compressed (the uncompressed size)
    size_t mUncompressedSize; // XXX valid, but not actually used
};


}

#endif
