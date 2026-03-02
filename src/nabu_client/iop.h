//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CLIENT_IOP_H
#define GICM_NABU_CLIENT_IOP_H

#include <memory>

#include <nabu/access.h>
#include <nabu/data_file.h>
#include <nabu/io_op.h>
#include <nabu/metakey.h>

#include <mcor/profiler.h>
#include <mcor/mtimer.h>
#include <mcor/url.h>


namespace nabu {

namespace client {

class Access;

/** class IOPImp :
 *
 */
class IOOperation : public nabu::IOOperation
{
public:
    IOOperation(const cor::Url& url,
                nabu::Access* access,
                const cor::TimeRange& timeRange,
                IterationDirection direction,
                const muuid::uuid& handle);
    virtual ~IOOperation();

    std::string GetHandle() const { return mHandle.string(); }


    // ==== Read operations

    // read up to count records
    void Read(const MetaKey& column,
              RecordVector& records,
              size_t count,
              cor::Time until = cor::Time::PositiveInf());

    // next record to read will be at or after whence
    bool ReadSeek(const MetaKey& column, const cor::Time& whence);

    void Search(const MetaKey& column,
                      const std::string& predicate,
                      nabu::SearchResultVector& results);

    // ==== Write Operations
    void Commit();
    void Abort();

    // write records
    void Write(const MetaKey& column, const RecordVector& records);

    const cor::TimeRange& TimeRange() const { return mTimeRange; }

    void Render(Json::Value& v);

    const cor::Url& GetURL()const { return mUrl; }

    // deletes the proxy on the far end
    void Close();

protected:
    const cor::Url mUrl;
    nabu::Access* mAccess;

    void CommitImp(bool abort);

    // XXX make default and copy constructors hidden
    //IOPImp();
    //IOPImp(const IOPImp& other);
    //void operator=(const IOPImp& other);
};

typedef std::shared_ptr<IOOperation> IOOperationPtr;

} // end namespaces
}

#endif
