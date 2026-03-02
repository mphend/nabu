//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_METADATA_TABLE_H
#define GICM_NABU_METADATA_TABLE_H


#include <string>
#include <map>
#include <memory>
#include <set>

#include <mcor/mfile.h>
#include <mcor/mmutex.h>
#include <mcor/timerange.h>

#include "async_status.h"
#include "interface_types.h"
#include "filenamer.h"
#include "garbage.h"
#include "metadata_map.h"
#include "metakey.h"
#include "table_address.h"
#include "time_path.h"
#include "time_scheme.h"
#include "version.h"


namespace nabu
{

class Database;

/** class MetaDataTableImp
 *
 */
class MetaDataTableImp
{
public:
    MetaDataTableImp();
    MetaDataTableImp(const UtcTimeScheme& timeScheme,
                     const TableAddress& address,
                     size_t layer);
    virtual ~MetaDataTableImp();

    bool Valid() const { return (bool)mMetaDataMap; }

    // returns current version for path
    // 'lookupKey' is for this object
    // 'fullPath' is the full time path of the data requested
    Version GetCurrentVersion(const DatabaseImp& database, const MetaKey& lookupKey, const nabu::TimePath& fullPath);

    // return the extents of data for the given version
    enum Side {eEarliest, eLatest, eBoth};
    cor::TimeRange Extents(const DatabaseImp& database, const Version& version, Side side);

    // ---- methods for root instances only

    // return the extents of writable data for given column. If there
    // is no data, TimeRange will be Invalid
    cor::TimeRange RootWriteExtents(DatabaseImp& database, const MetaKey& column);

    // commit the given changes (from an IOOperation) to metadata and persist
    // XXX this and Commit might be better placed in WriteOperation
    void RootCommit(DatabaseImp& database,
                    const MetaKey& column,
                    const std::map<TimePath, Garbage::Entry>& writtenLeaves,
                    const cor::TimeRange& extent);

    // sets timePath to point to first data in column either before
    // (eIterateBackwards) or after (eIterateForwards) the time 'mark'
    // returns true if any data exists, false otherwise
    bool RootFind(const DatabaseImp& database,
                  TimePath& timePath,
                  const MetaKey& column,
                  IterationDirection direction,
                  const cor::Time& mark) const;
    bool RootNext(const DatabaseImp& database, TimePath& timePath, const MetaKey& column) const
    {
        return RootAdvance(database, timePath, column, eIterateForwards);
    }
    bool RootPrevious(const DatabaseImp& database, TimePath& timePath, const MetaKey& column) const
    {
        return RootAdvance(database, timePath, column, eIterateBackwards);
    }
    bool RootAdvance(const DatabaseImp& database, TimePath& timePath, const MetaKey& column, IterationDirection direction) const;

    // returns the columns, if any, under column, which must be address-type
    // metakey.
    void RootGetColumns(const MetaKey& column, std::vector<MetaKey>& columns);

    // returns whether the column exists
    bool HasColumn(const MetaKey& column) const;

    // return the table at the given column and metadata table, loading it from
    // disk if possible. If there is no such table, a new one is returned.
    std::shared_ptr<MetaDataTableImp> GetTable(const Database& database,
                                               const MetaKey& column,
                                               const MetaData& metaData) const;

    struct VersionPair
    {
        Version mOld;
        Version mNew;
    };
    typedef std::map<TableAddress, VersionPair> TouchedMetaDataMap;
    // Update metadata for the given written leaf and its version.
    // 'touched' map is keyed by layer number to visit the deepest
    // metadata first for interruption safety
    void Commit(const DatabaseImp& database,
                const MetaKey& column,
                const TimePath& writtenLeaf,
                const Version& leafVersion,
                std::map<size_t, TouchedMetaDataMap>& touched);
    // remove metadata for given leafs; returns number of leafs removed
    size_t Remove(const DatabaseImp& database,
                  const MetaKey& column,
                  const TimePath& erasedLeaf,
                  std::map<size_t, TouchedMetaDataMap>& touched,
                  std::set<Garbage::Entry>& removedFiles);

    // initialize a timePath by finding the given item at start; index into
    // TimePath start is passed in for efficiency (so to not have to manipulate it)
    bool Find(const DatabaseImp& database,
              TimePath& timePathReversed,
              size_t index,
              IterationDirection direction,
              const TimePath& start) const;

    // advance timePath to either the next or previous valid value
    bool Advance(const DatabaseImp& database, TimePath& timePath, size_t index, IterationDirection direction) const;

    void PrintAll(const DatabaseImp& database, std::ostream& os, unsigned indent = 0);

    size_t Size() const { return mMetaDataMap->size(); }
    // descends tree to compute total size from this node down
    size_t TotalSize(const DatabaseImp& database);
    size_t Empty() const { return mMetaDataMap->empty(); }

    // save this object to a file
    void Persist(const Database& database);
    // loads this object; returns true if successful, false if this metadata does
    // not exist
    bool Load(const Database& database);

    void LoadJson(const Json::Value& root);
    void SaveJson(Json::Value& root) const;
    void LoadDocument(const std::string& document);
    void SaveDocument(std::string& document) const;

    std::string GetLiteral() const { return GetTableAddress().GetLiteral(); }

    const TimePath& GetPath() const { return mTableAddress.GetTimePath(); }
    const MetaKey& GetColumn() const { return mTableAddress.GetColumn(); }
    const TableAddress& GetTableAddress() const { return mTableAddress; }

    MetaDataTableImp* Replicate();

    // return whether this is a root or not
    virtual bool IsRoot() const { return mIsRoot; }
    virtual bool IsLeaf() const { return mLayer == 1; }

    // iterate contents and delete any node not otherwise in use in the database;
    // returns number of nodes removed
    size_t DeleteUnused(DatabaseImp& database);

    CopyStats RootPullUniqueNodes(const Database& source,
                                  DatabaseImp& destination);
    CopyStats PullUniqueNodes(const Database& source,
                              DatabaseImp& destination);

    CopyStats RootPushUniqueNodes(const DatabaseImp& source,
                              Database& destination);
    CopyStats PushUniqueNodes(const DatabaseImp& source,
                              Database& destination);

    // count the number of missing files referenced by this table and its
    // children; the number could be higher, since once metadata is corrupted,
    // all bets are off.
    void RootCountMissing(const DatabaseImp& database, AsyncStatus& stats);
    void CountMissing(const DatabaseImp& database, AsyncStatus& stats);

protected:
    // There is no mutex or other synchronization for the data of this
    // class. See .cpp file for details.
    UtcTimeScheme mTimeScheme;
    TableAddress mTableAddress;

    // this is only used for defensive purposes, to guard that a table,
    // once persisted, is never changed
    bool mPersisted;

    // layer of this object itself
    size_t mLayer;
    bool mIsRoot; // is a root
    size_t GetNumberOfLayers();

    // some semantics on layers to avoid confusion
    size_t LayerOfThis() const { return mLayer; }
    size_t LayerAddressed() const { return mLayer - 1; }

    // metadata contents
    MetaDataMap mMetaDataMap;
};

typedef std::shared_ptr<MetaDataTableImp> MetaDataTable;

} // end namespace

#endif
