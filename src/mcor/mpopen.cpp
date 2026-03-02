//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <string.h>
#include <map>

#include "mpopen.h"


namespace cor {

class ModeStringMap : public std::map<Popen::Mode, std::string>
{
public:
    ModeStringMap()
    {
        (*this)[Popen::eRead] = "r";
        (*this)[Popen::eWrite] = "w";
    }
} modeStringMap;

Popen::Popen(const std::string& command, Mode mode) :
    mFp(0),
    mCommand(command),
    mMode(mode)
{}

Popen::~Popen()
{
    Close();
}

std::string
Popen::Read()
{
    Open();

    std::string r;

    char buffer[128];
    while (fgets(buffer, sizeof(buffer), mFp) != NULL)
    {
        r += buffer;
    }

    return r;
}

bool
Popen::Read(std::string& s)
{
    Open();

    char buffer[128];
    s.clear();
    while (true)
    {
        if (fgets(buffer, sizeof(buffer), mFp) != buffer)
            return false; // EOF

        s += buffer;

        if (strlen(buffer) != 127)
            return true; // new line
    }
}

void
Popen::Write(const std::string& s)
{
    Open();

    if (fputs(s.c_str(), mFp) == EOF)
        throw cor::Exception("Unable to write to command '%s'", mCommand.c_str());
}

void
Popen::Open()
{
    if (mFp == 0)
    {
        mFp = popen(mCommand.c_str(), modeStringMap[mMode].c_str());
        if (mFp == NULL)
            throw cor::ErrException("Could not execute '%s'", mCommand.c_str());
    }
}

void
Popen::Close()
{
    if (mFp != 0)
    {
        mRc = pclose(mFp);
    }
    mFp = 0;
}

void
Popen::Assert()
{
    if (mFp != 0)
        throw cor::Exception("Popen::Assert() called when stream not closed");
    if (mRc != 0)
        throw cor::ErrException("Popen::Assert ()");
}
    
}
