//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CONFIG_H
#define GICM_NABU_CONFIG_H

#include <string>
#include <map>
#include <set>
#include <vector>

#include <mcor/muuid.h>
#include <mcor/mexception.h>

#include "time_scheme.h"

namespace nabu {

/** Class Config
 *      The configuration of the database
 *
 */


class Config
{
public:
    Config(const std::string& rootDirectory);
    ~Config();

    // returns whether a config appears to exist or not
    bool Exists() const;

    // locates and reads the configuration file
    void Parse();

    // create an initial configuration file; will throw if one already exits
    void Create(const UtcTimeScheme& timeScheme, const muuid::uuid& fingerprint);

    // create an initial configuration file if one does not already exist;
    // returns true if a new file is created, false otherwise
    bool TryCreate(const UtcTimeScheme& timeScheme, const muuid::uuid& fingerprint);

    // ---- configuration access

    UtcTimeScheme GetTimeScheme() const;

    muuid::uuid GetFingerprint() const;

private:
    std::string mRootDirectory;
    Json::Value mValue;

    std::string GetConfigFileName() const;

    std::string ReadString(const std::string& key) const;

};

}

#endif
