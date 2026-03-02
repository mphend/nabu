//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_DATABASE_H
#define GICM_NABU_SERVER_DATABASE_H


#include <nabu/database_imp.h>

namespace nabu {
namespace server {


/** class Database
 *
 */
class Database : public DatabaseImp
{
public:
    Database(const std::string& rootDirectory, int accessTimeoutSeconds);

    ~Database();

    AccessPtr CreateAccess(const MetaKey& branchPath,
                           const cor::TimeRange& timeRange,
                           AccessType type,
                           const std::string& tagName = "");
protected:
    int mAccessTimeoutSeconds;
};

}
}
#endif
