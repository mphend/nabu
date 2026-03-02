//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_JOURNAL__
#define __PKG_NABU_JOURNAL__

#include <map>
#include <memory>
#include <vector>

#include <mcor/mmutex.h>
#include <mcor/timerange.h>

#include "record.h"
#include "metakey.h"

namespace nabu
{

class BranchImp;
class DatabaseImp;


/** class Journal :
 *
 */

class JournalImp
{
public:
    JournalImp(BranchImp* branch, const MetaKey& column);

    // construct a journal just to parse a file; this object cannot
    // behave fully like a journal, for instance, be Clear()ed, etc.
    JournalImp(const std::string& fileName);

    ~JournalImp();

    // The first record to be returned will be the second after the beginning
    // of timeRange
    void Read(const cor::TimeRange& timeRange, RecordVector& records);
    bool Read(const cor::Time& time, Record& records);
    void Append(RecordVector& records);

    cor::TimeRange Extents() const;
    cor::Time GetLastTime() const { return Extents().Final(); }

    // loads journal from disk, dealing with possibly interrupted
    // clear
    void Load();

    // clear all records up to and including final, rendering a new
    // journal file
    void Clear(const cor::Time& final);

protected:
    BranchImp* mBranch;
    const MetaKey mColumnAddress;

    mutable cor::MMutex mMutex;
    typedef std::map<cor::Time, Record> RecordMap;
    RecordMap mRecords;

    void ParseFile(const std::string& filename);
    void WriteRecord(cor::File& file, Record record);
    void WriteHeader(cor::File& file);

};

typedef std::shared_ptr<JournalImp> Journal;
typedef std::map<MetaKey, Journal> JournalMap;

}

#endif
