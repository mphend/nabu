//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_LEAK_DETECTOR_H
#define GICM_NABU_LEAK_DETECTOR_H

#include <string>
#include <map>

#include <mcor/mmutex.h>

namespace nabu
{

class LeakDetector
{
public:
    static LeakDetector& Get();

    void Increment(const std::string& type);
    void Decrement(const std::string& type);

    // print all counts
    void Dump();

private:
    LeakDetector();
    ~LeakDetector();

    mutable cor::MMutex mMutex;

    // key is type
    // value is count
    typedef std::map<std::string, long> Imp;
    Imp mImp;
};


} // end namespace

#endif
