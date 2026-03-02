//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CLIENT_NABU_PROXY_H
#define GICM_NABU_CLIENT_NABU_PROXY_H

#include <nabu/database.h>
#include <nabu/interface_types.h>
#include <nabu_client/iop.h>

#include <mcor/url.h>

namespace nabu {
namespace client {

class Branch;

/** Function GetDatabases
 *
 *  Queries the server to find the set of databases being served
 */
void
GetDatabases(const cor::Url& url, std::set<std::string>& instances);


/** class Database : a proxy to a remote database
 *
 */
class Database : public nabu::Database
{
public:
    // url is where the server is; instanceName is the specific database
    // that is being referenced at the server
    //     url is "http://dasi.nabu.cpidatalab.com/nabu/v1", for instance
    //     instanceName is "mag_data"
    Database(const cor::Url& url, const std::string& instanceName);
    virtual ~Database();

    const std::string& GetInstanceName() const { return mInstance; }

    std::string GetDescriptiveLiteral() const
    {
        return mUrl.GetLiteral() + "/"  + GetInstanceName();
    }

    nabu::BranchPtr GetBranch(const MetaKey& branchPath);
    nabu::BranchPtr GetMain();
    nabu::BranchPtr CreateBranch(const MetaKey& branchPath);
    void DeleteBranch(const MetaKey& branchPath);


    nabu::AccessPtr CreateAccess(const MetaKey& branchPath,
                                   const cor::TimeRange& timeRange,
                                   AccessType type,
                                   const std::string tagName = "");

    bool VersionInUse(const TableAddress& nodeAddress);

    void GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs);

    void WriteFile(const std::vector<unsigned char>& data,
                           const TableAddress& tableAddress,
                           FileType fileType);
    void ReadFile(std::vector<unsigned char>& data,
                          const TableAddress& tableAddress,
                          FileType fileType) const;

    bool GetMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const;
    void PersistMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const;

    std::vector<unsigned int> GetPeriods() const;
    muuid::uuid GetFingerprint() const;

    // return number of seconds for IOOperationPtr and Access timeout
    int GetTimeout();

protected:
    const std::string mInstance;
};


/** class DuplicateTag : duplicates a tag
 */
class DuplicateTag
{
public:
    DuplicateTag(const cor::Url& url) : mUrl(url) {}
    virtual ~DuplicateTag() {}

    void Duplicate(const std::string& existingTagName, const std::string& newTagName);

protected:
    const cor::Url mUrl;
};


/** class DeleteTag : deletes a tag
 */
class DeleteTag
{
public:
    DeleteTag(const cor::Url& url) : mUrl(url) {}
    virtual ~DeleteTag() {}

    void Delete(const std::string& tagName);

protected:
    const cor::Url mUrl;
};


}
}

#endif
