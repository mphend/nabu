//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <algorithm>
#include <fstream>
#include <locale>


#include "options.h"



std::string
ProgOptions::NabuDirectory()
{
    return ReadString("NabuDirectory");
}

int
ProgOptions::WebPortNumber() const
{
    return ReadInt("WebPortNumber");
}

std::string
ProgOptions::Prefix() const
{
    return ReadString("Prefix");
}

int
ProgOptions::CacheSizeLimit() const
{
    return ReadInt("CacheLimit");
}

int
ProgOptions::IOAccessTimeoutSeconds() const
{
    return ReadInt("IOAccessTimeoutSeconds");
}

int
ProgOptions::SelectBlockPeriodSeconds() const
{
    return ReadInt("SelectBlockPeriodSeconds");
}

// log level
std::string
ProgOptions::LogLevel() const
{
    return ReadString("LogLevel");
}

bool
ProgOptions::LogEcho() const
{
    return ReadBool("LogEcho");
}

std::vector<nabu::MetaKey>
ProgOptions::SimulationColumns() const
{
    std::vector<nabu::MetaKey> r;
    if (!HasMember("SimulationColumns"))
        return r;

    const Json::Value& array = GetJson("SimulationColumns");
    if (!array.isArray())
        throw cor::Exception("Expected an Array for 'SimulationColumns'");

    Json::ValueConstIterator i = array.begin();
    for (; i != array.end(); i++)
    {
        r.push_back(nabu::MetaKey((*i).asString()));
    }

    return r;
}

int
ProgOptions::SimulationChunk() const
{
    return ReadInt("SimulationChunk");
}

int
ProgOptions::SimulationBackfillSeconds() const
{
    return ReadInt("SimulationBackfillSeconds");
}

ProgOptions& Options()
{
    static ProgOptions p;

    return p;
}
