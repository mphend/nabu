//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include "menvironment.h"

#include <stdlib.h>
#include <unistd.h>

#include "strutil.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace cor {

MMutex Environment::staticEnvironmentMutex("environment mutex");

Environment&
Environment::GetInstance(cor::ObjectLocker& locker)
{
    static Environment imp;
    locker.Subsume(Environment::staticEnvironmentMutex);
    return imp;
}

Environment::Environment() {}
Environment::~Environment() {}

void
Environment::Get(std::map<std::string, std::string>& out)
{
    out.clear();
    char** line = environ;
    while (*line != 0)
    {
        std::string l(*line);
        std::string::size_type i = l.find('=');
        if (i == std::string::npos)
            throw cor::Exception("Error in environment string '%s'\n", *line);
        out[l.substr(0,i)] = l.substr(i + 1);
        line++;
    }
}

std::string
Environment::Get(const std::string& key)
{
    char* r = getenv(key.c_str());
    if (r == 0)
        throw cor::Exception("No such environment value '%s'", key.c_str());
    return r;
}

bool
Environment::Has(const std::string& key)
{
    return getenv(key.c_str()) != 0;
}

void
Environment::Set(const std::string& key, const std::string& value)
{
    if (key.empty())
        throw cor::Exception("Environment::Set() -- empty key");
    if (key.find('=') != std::string::npos)
        throw cor::Exception("Environment::Set() -- key has '=' character");
    if (setenv(key.c_str(), value.c_str(), 1) == -1)
        throw cor::ErrException("Environment::Set %s=%s\n", key.c_str(), value.c_str());
}

void
Environment::Clear(const std::string& key)
{
    clearenv();
}

}
