//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_MCOR_ENVIRO_INCLUDED
#define PKG_MCOR_ENVIRO_INCLUDED


#include <stdint.h>

#include <string>
#include <map>

#include "mmutex.h"

namespace cor
{


/** class Environment : threadsafe and error-checked environment access
 *
 *  Keep in mind this common misunderstanding of the environment; these calls
 *  can only change the environment of the current process, which is inherited
 *  by child processes. Children cannot change the environment of their
 *  parent. So changes to the environment will not appear to persist or be
 *  visible to the parent.
 *
 */

class Environment
{
public:
    static Environment& GetInstance(cor::ObjectLocker& locker);

    // return the current environment as a map
    void Get(std::map<std::string, std::string>& out);

    // return value for key; throws if not found
    std::string Get(const std::string& key);

    // returns whether key exists in the environment
    bool Has(const std::string& key);

    // sets the key to value
    void Set(const std::string& key, const std::string& value);

    // clears the key; if key does not exist, does nothing
    void Clear(const std::string& key);

private:
    Environment();
    virtual ~Environment();

    static MMutex staticEnvironmentMutex;

};

}

#endif
