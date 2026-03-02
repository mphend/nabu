//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __pkg_mcor_arg_map__
#define __pkg_mcor_arg_map__

#include <map>
#include <vector>

#include "mtime.h"

namespace cor
{

class ArgException : public cor::Exception
{
public:
    ArgException(const cor::Exception& err) throw()
    {
        mWhat = std::string(err.what());
    }
    virtual ~ArgException() throw() {}
};

/** class ArgMap : helps parse command-line args which are of format:
 *    --flag1 value1 value2 --flag2 value1 --flag3 --flag4 value1, ...
 *
 */
class
ArgMap
{
public:
    ArgMap();
    ~ArgMap();

    // arg 'index' is the index of the first argv[] that should be parsed as an argument;
    // for most shells, this should be 1 since the command itself is at index 0.
    void Parse(const char* const argv[], int argc, size_t firstArgIndex = 1);

    // if --key exists (note hyphens are not part of 'key') returns number of
    // args, including the key itself, and moves row index to this key; resets
    // sequence index to zero
    size_t Find(const std::string& key);

    // return next sequential arg as string
    const std::string& Next();
    // return next sequential arg as integer
    int NextInteger();
    // return next sequential arg as double
    double NextDouble();

    // get next date in 2023-7-1 format
    cor::Time NextDate();
    // get next time in 13:01:13 format
    cor::Time NextTime();
    // get next date and time in 2023-7-1 13:01:13 format
    cor::Time NextDateAndTime();

    const std::string& Get(size_t arg);
    const int GetInteger(size_t arg);
    const double GetDouble(size_t arg);

    std::ostream& StreamIt(std::ostream& os) const;
protected:
    typedef std::map<std::string, std::vector<std::string> > BaseT;
    BaseT mImp;
    BaseT::const_iterator mRow;
    size_t mNext;

};

std::ostream& operator<<(std::ostream& os, const ArgMap& am);

} // end namespace

#endif
