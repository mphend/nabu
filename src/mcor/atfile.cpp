//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include "atfile.h"
#include <mcor/strutil.h>


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


ATFile::ATFile(cor::File& file) : File(file)
{
}

ATFile::~ATFile()
{
}


void
ATFile::Send(const std::string& send)
{
    //printf("Tx: %s\n", send.c_str());
    WriteAll((send + "\r\n").c_str(), send.length() + 2);
}

std::string
ATFile::Expect(const std::string& expected, double timeout)
{
    static char rxBuffer[8000];

    size_t n = 0;

    while (timeout)
    {
        size_t i = ReadAll(rxBuffer + n, sizeof(rxBuffer), timeout);
        if (i != 0)
        {
            if (!expected.empty())
            {
                rxBuffer[n + i] = 0;
                std::string s(rxBuffer);
                //printf("Rx %d: '%s'\n", i, s.c_str());
                //printf("%s\n", cor::HexDump(s.data(), s.length()).c_str());

                std::string::size_type termPos = s.find(expected, n);
                if (termPos != std::string::npos)
                {
                    n = termPos;
                    break;
                }
            }
            n += i;
        }
        else
        {
            if (!expected.empty())
                throw cor::Exception("Timeout waiting for expected string '%s'", expected.c_str());
            else
                break; // timeout
        }
    }

    rxBuffer[n] = 0;
    std::string returned = std::string(rxBuffer);

    return returned;
}

std::string
ATFile::Command(const std::string& cmd)
{
    std::string s;
    try
    {
        Send(cmd);
        s = Expect("OK");
    }
    catch (const cor::Exception& err)
    {
        throw cor::Exception("Command %s: %s", cmd.c_str(), err.what());
    }

    std::string::size_type cmdBegin = s.find(cmd);
    if (cmdBegin == std::string::npos)
        throw cor::Exception("Missing echo for command '%s'", cmd.c_str());
    cmdBegin += cmd.length();

    std::string::size_type begin = s.find_first_not_of(" \r\t\n", cmdBegin);
    if (begin == std::string::npos)
        return ""; // response was all whitespace
    std::string::size_type end = s.find_last_not_of(" \r\t\n");
    if (end == std::string::npos)
        return s.substr(begin, end); // all of remainder is whitespace

    return s.substr(begin, end-begin+1);
}

