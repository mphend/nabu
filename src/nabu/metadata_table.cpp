//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <algorithm>

#include <mcor/json_config.h>

#include "cache.h"
#include "context.h"
#include "database_imp.h"
#include "exception.h"
#include "io_op_imp.h"
#include "leak_detector.h"
#include "metadata_table.h"
#include "profiler.h"
#include "mcor/json_config.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu
{

// Note about lack of a Mutex or ReadWrite lock:
//
// This class does not need a mutex to protect its data, because it is
// only written on Commit, and otherwise read-only. The Commit occurs
// when the version of the table is first created, and during this
// commit the Branch's ReadWrite lock is held, guaranteeing single
// access. Threads that enter after that lock is released will
// call Commit on a different version (instance) of the table.
//
// If there were locking for this class, then deadlock would need to be
// avoided by some strategy, possibly by use of a ReadWrite lock. One
// example of such deadlock is a tag at version N+1 being compared to
// a tag at version N, simultaneous to a tag at version N is being compared to a
// tag at version N+1. This operation looks like:
//   (N+1).Compare(N)
//   N.Compare(N+1)
// You can see the order reversal.

MetaDataTableImp::MetaDataTableImp() :
    mLayer(0),
    mIsRoot(true),
    mPersisted(false)
{
}

MetaDataTableImp::MetaDataTableImp(const UtcTimeScheme& timeScheme, const TableAddress& address, size_t layer) :
        mTimeScheme(timeScheme),
        mLayer(layer),
        mIsRoot(mTimeScheme.RootLayer() == mLayer),
        mTableAddress(address),
        mPersisted(false),
        mMetaDataMap(new MetaDataMapImp(address.IsRoot() ? MetaKey::eAddress : MetaKey::eChunk))
{
    LeakDetector::Get().Increment("MetaDataTableImp");
    DEBUG_LOCAL("MetaDataTableImp constructor -- %s layer %ld\n", mTableAddress.GetLiteral().c_str(), mLayer);
}


MetaDataTableImp::~MetaDataTableImp()
{
    LeakDetector::Get().Decrement("MetaDataTableImp");
    DEBUG_LOCAL("MetaDataTableImp destructor -- %s layer %ld\n", mTableAddress.GetLiteral().c_str(), mLayer);
}


Version
MetaDataTableImp::GetCurrentVersion(const DatabaseImp& database,
                                    const MetaKey& lookupKey,
                                    const nabu::TimePath& fullPath)
{
    try {

        DEBUG_LOCAL("MetaDataTableImp::GetCurrentVersion -- %s (layer %ld), resolving %s:%s\n",
                    GetLiteral().c_str(),
                    mLayer,
                    lookupKey.GetLiteral().c_str(),
                    fullPath.GetLiteral().c_str());

    CacheLocker cl(this);

    // see if this is request for the root itself, which must be
    // this
    TableAddress ta(lookupKey, fullPath, Version());
    if (ta.IsRoot())
    {
        // DEFENSIVE
        if (!IsRoot()) // but 'this' is not root
            throw cor::Exception("MetaDataTableImp::GetCurrentVersion -- arguments request root, but this is a non-root (%s) table\n",
                GetLiteral().c_str());
        return mTableAddress.GetVersion();
    }

    MetaDataMapImp::const_iterator i = mMetaDataMap->find(lookupKey);

    if (i == mMetaDataMap->end())
    {
        //printf("MetaDataTableImp::GetCurrentVersion -- no data at key %s\n", lookupKey.GetLiteral().c_str());
        return Version(); // miss
    }

    // unless this is the requested version layer, invoke this call hierarchically
    // to the next layer down, with the correct key and version for that layer
    if ((GetNumberOfLayers() - fullPath.size() + 1) == (LayerOfThis()))
    {
        return i->second.mVersion;
    }
    else
    {
        MetaDataTable sub;
        if (IsRoot())
            sub = GetTable(database, lookupKey, i->second);
        else
            sub = GetTable(database, mTableAddress.GetColumn(), i->second);

        // DEFENSIVE
        if (sub.get() == this)
        {
            throw FatalException(database.GetUrl(),
                                 "MetaDataTableImp::GetCurrentVersion -- breaking infinite loop");
        }

        size_t pathIndex = LayerAddressed() - 1; // next layer down's time chunk
        pathIndex -= GetNumberOfLayers() - fullPath.size(); // if partial time address for non-leaf is requested
        Chunk subordinateKey = fullPath.at(pathIndex);
        return sub->GetCurrentVersion(database, subordinateKey, fullPath);
    }

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTableImp::GetCurrentVersion() -- %s", err.what());
        throw err;
    }
}

cor::TimeRange
MetaDataTableImp::Extents(const DatabaseImp& database, const Version& version, Side side)
{
    try {

        DEBUG_LOCAL("MetaDataTableImp::Extents layer %ld, table %s, request version %s, %s side\n",
               mLayer,
               GetLiteral().c_str(),
               version.GetLiteral().c_str(),
               side == eEarliest ? "earliest" : side == eLatest ? "latest" : "both");

        CacheLocker cl(this);

        // DEFENSIVE
        if (mMetaDataMap->empty())
        {
            throw cor::Exception("MetaDataTableImp::Extents() -- Empty metadata map at %s", GetLiteral().c_str());
        }

        MetaDataMapImp::const_iterator firstEntry = mMetaDataMap->begin();
        MetaDataMapImp::const_iterator finalEntry = mMetaDataMap->end();
        finalEntry--;

        // unless we are a leaf, invoke this call hierarchically to the next layer
        // down, with the correct key and version for that layer
        if (IsLeaf())
        {
            cor::Time first, final;
            {
                if (side != eLatest)
                {
                    std::string fileName = database.GetFileNamer().NameOf(TableAddress(mTableAddress.GetColumn(),
                                                                   firstEntry->second.mPath,
                                                                   firstEntry->second.mVersion),
                                                                   FileType::eData);
                    GetProfiler().Start("Extents.ReadData");
                    DataFile dataFile(fileName);

                    dataFile.ReadHeaderOnly();
                    if (dataFile.TimeRange().Valid())
                    {
                        first = dataFile.TimeRange().First();
                        final = dataFile.TimeRange().Final();
                    }
                    else
                    {
                        // old version; got to read the whole file
                        RecordVector records;
                        dataFile.Read(records, eIterateForwards);
                        first = records.front().GetTime();
                        final = records.back().GetTime();
                    }
                    GetProfiler().Stop("Extents.ReadData");
                }

                if (side != eEarliest)
                {
                    std::string fileName = database.GetFileNamer().NameOf(TableAddress(mTableAddress.GetColumn(),
                                                                   finalEntry->second.mPath,
                                                                   finalEntry->second.mVersion),
                                                                   FileType::eData);
                    GetProfiler().Start("Extents.ReadData");
                    DataFile dataFile(fileName);
                    dataFile.ReadHeaderOnly();

                    if (dataFile.TimeRange().Valid())
                    {
                        final = dataFile.TimeRange().Final();
                    }
                    else
                    {
                        // old version; got to read the whole file
                        RecordVector records;
                        dataFile.Read(records, eIterateForwards);
                        final = records.back().GetTime();
                    }

                    GetProfiler().Stop("Extents.ReadData");
                }
            }

            cor::TimeRange tr(first, final);
            //printf("%s: extents are %s\n", GetLiteral().c_str(), tr.Print().c_str());
            return tr;
        }
        else
        {
            MetaDataTable firstSub = GetTable(database, mTableAddress.GetColumn(), firstEntry->second);
            MetaDataTable finalSub = GetTable(database, mTableAddress.GetColumn(), finalEntry->second);

            // DEFENSIVE
            if ((firstSub.get() == this) || (finalSub.get() == this))
            {
                throw FatalException(database.GetUrl(),
                                     "MetaDataTableImp::Extents -- breaking infinite loop");
            }

            // optimize for single entry
            if (firstSub == finalSub)
                return cor::TimeRange(firstSub->Extents(database, firstEntry->second.mVersion, side));

            if (side == eBoth)
                return cor::TimeRange(
                        firstSub->Extents(database, firstEntry->second.mVersion, eEarliest).First(),
                        finalSub->Extents(database, finalEntry->second.mVersion, eLatest).Final());
            else if (side == eEarliest)
                return firstSub->Extents(database, firstEntry->second.mVersion, eEarliest);
            // eLatest
            return finalSub->Extents(database, finalEntry->second.mVersion, eLatest);
        }

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTable::Extents() -- %s", err.what());
        throw err;
    }
}

cor::TimeRange
MetaDataTableImp::RootWriteExtents(DatabaseImp& database, const MetaKey& column)
{
    try {

        DEBUG_LOCAL("MetaDataTableImp::RootWriteExtents %s\n",
                    column.GetLiteral().c_str());

        CacheLocker cl(this);

        MetaDataMapImp::iterator i = mMetaDataMap->find(column);
        if (i == mMetaDataMap->end())
        {
            return cor::TimeRange(); // no data
        }

        MetaDataTable sub = GetTable(database, column, i->second);
        cor::TimeRange tr = sub->Extents(database, i->second.mVersion, eBoth);

        return tr;

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTableImp::RootWriteExtents() %s -- %s", column.GetLiteral().c_str(), err.what());
        PrintAll(database, std::cout);
        throw err;
    }
}

void
MetaDataTableImp::RootCommit(DatabaseImp& database,
                             const MetaKey& column,
                             const std::map<TimePath, Garbage::Entry>& writtenLeaves,
                             const cor::TimeRange& extent)
{
    try {
        Context ctx("MetaDataTable::RootCommit %s %s",
                    column.GetLiteral().c_str(),
                    extent.Print().c_str());
        DEBUG_LOCAL("MetaDataTableImp::RootCommit %s:%s committing %ld leaves\n",
                    mTableAddress.GetColumn().GetLiteral().c_str(),
                    mTableAddress.GetTimePath().GetLiteral().c_str(),
                    writtenLeaves.size());

        // * This is always invoked on the newly replicated root node
        // * Walk the tree of written nodes, updating leaf metadata versions
        //   and adding paths to visited metadata tables along with the
        //   incremented version number for that table to a set in the process
        // * Iterate the set and persist each table with the version


        // this is the newly replicated node, so creat a new version
        TableAddress garbageAddress = GetTableAddress();
        mTableAddress.NewVersion();

        std::map<size_t, TouchedMetaDataMap> touchedMetaData;

        CacheDisabler cacheDisabled;

        size_t added = 0;
        // commit written files
        {
            std::map<TimePath, Garbage::Entry>::const_iterator i = writtenLeaves.begin();

            for (; i != writtenLeaves.end(); i++)
            {
                MetaDataTableImp::Commit(database, column, i->first, i->second.mTableAddress.GetVersion(), touchedMetaData);
            }
        }

        std::set<Garbage::Entry> garbage;

        // commit removed files
        size_t removed = 0;

        /*printf("Written leaves\n");
        std::map<TimePath, Garbage::Entry>::const_iterator wli = writtenLeaves.begin();
        for (; wli != writtenLeaves.end(); wli++)
        {
            printf("%s = %s\n", wli->first.GetLiteral().c_str(), wli->second.Print().c_str());
        }*/

        TimePath timePath;
        if (RootFind(database, timePath, column, eIterateForwards, extent.First()))
        {
            //printf("initial timePath = %s\n", timePath.GetLiteral().c_str());
            while (timePath.Valid())
            {
                // check for going past the extents
                cor::Time tpBegin = database.GetTimeScheme().GetTime(timePath).First();
                //printf("tpBegin = %s, extent = %s\n", tpBegin.Print().c_str(), extent.Print().c_str());
                if (tpBegin > extent.Final())
                {
                    break;
                }

                // skip if this was written; otherwise it was erased
                if (writtenLeaves.find(timePath) != writtenLeaves.end())
                {
                    RootNext(database, timePath, column);
                    continue;
                }
                //printf("%s not written\n", timePath.GetLiteral().c_str());

                // get next timePath now, because it will not exist after Remove
                TimePath next = timePath;
                RootNext(database, next, column);
                //printf("removing %s\n", timePath.GetLiteral().c_str());
                
                MetaDataTableImp::Remove(database, column, timePath, touchedMetaData, garbage);
                timePath = next;

            }
        }

        /*printf("Adjusted metadata\n");
        for (size_t i = 0; i < touchedMetaData.size(); i++)
        {
            TouchedMetaDataMap::const_iterator j = touchedMetaData[i].begin();
            for (; j != touchedMetaData[i].end(); j++)
            {
                printf("%s : %s -> %s\n",
                       j->first.GetLiteral().c_str(),
                       j->second.mOld.GetLiteral().c_str(),
                       j->second.mNew.GetLiteral().c_str());
            }
        }*/

        // make sure all data is on disk before touching metadata
        cor::File::SyncAll();

        // persist touched metadata
        {
            std::map<size_t, TouchedMetaDataMap>::const_iterator i = touchedMetaData.begin();
            for (; i != touchedMetaData.end(); i++)
            {
                //printf("Committing layer %ld\n", i->first);
                TouchedMetaDataMap::const_iterator j = i->second.begin();
                for (; j != i->second.end(); j++)
                {
                    TableAddress ta = j->first;
                    ta.SetVersion(j->second.mNew);
                    MetaDataTable table = Cache::Get().Find(database, ta);

                    // DEFENSIVE
                    if (!table)
                        throw cor::Exception("Empty table in commit");
                    if (table->mMetaDataMap->empty())
                    {
                        throw cor::Exception("Persisting a table (%s: %s) with no entries",
                                             table->mTableAddress.GetLiteral().c_str(),
                                             database.GetTimeScheme().GetTime(table->GetPath()).Print().c_str());
                    }
                    table->Persist(database);
                    TableAddress garbageTA = table->GetTableAddress();
                    garbageTA.SetVersion(j->second.mOld);
                    Garbage::Entry ge(garbageTA, FileType::eMetaData);

                    garbage.insert(ge);
                }
            }
        }

        // persist this root itself
        Persist(database);

        // the old version of this root is now potential garbage
        garbage.insert(Garbage::Entry(garbageAddress, FileType::eMetaData));

        // safe to delete garbage created by this commit
        database.GetGarbage().Insert(garbage);

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTableImp::RootCommit() -- %s", err.what());
        throw err;
    }
}

bool
MetaDataTableImp::RootFind(const DatabaseImp& database,
                           TimePath& timePath,
                           const MetaKey& column,
                           IterationDirection direction,
                           const cor::Time& mark)  const
{
    try {

        DEBUG_LOCAL("MetaDataTableImp::RootFind %s %s %s\n",
                    column.GetLiteral().c_str(),
                    direction == eIterateForwards? "after" : "before",
                    mark.Print().c_str());

        CacheLocker cl(this);
        MetaDataMapImp::iterator i = mMetaDataMap->find(column);
        if (i == mMetaDataMap->end())
        {
            //printf("RootFind : column '%s' does not exist\n", column.GetLiteral().c_str());
            return false;
        }

        timePath.clear();

        TimePath start = mTimeScheme.GetPath(mark);
        //printf("Searching for time %s, which is TimePath %s\n", mark.Print().c_str(), start.GetLiteral().c_str());

        MetaDataTable sub = GetTable(database, column, i->second);

        bool r = sub->Find(database, timePath, 0, direction, start);

        //printf("Root -- ascending\n");
        timePath.Reverse();
        return r;

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTableImp::RootFind() -- %s", err.what());
        throw err;
    }
}

bool
MetaDataTableImp::RootAdvance(const DatabaseImp& database,
                              TimePath& timePath,
                              const MetaKey& column,
                              IterationDirection direction) const
{
    DEBUG_LOCAL("MetaDataTableImp::RootAdvance  %s:%s %s\n",
                column.GetLiteral().c_str(),
                timePath.GetLiteral().c_str(),
                direction == eIterateForwards ? "forwards" : "backwards");

    std::reverse(timePath.begin(), timePath.end());

    try {

        CacheLocker cl(this);

        // DEFENSIVE
        if (timePath.empty())
            throw cor::Exception("Empty timePath passed to MetaDataTableImp::RootNext");

        // make sure column exists at all
        if (mMetaDataMap->find(column) == mMetaDataMap->end())
            return false;

        MetaDataTable columnRoot = GetTable(database, column, (*mMetaDataMap)[column]);

        // DEFENSIVE
        if (!columnRoot)
            throw cor::Exception("MetaDataTableImp::RootNext() -- subordinate table is NULL");


        //printf("MetaDataTableImp -- descending\n");
        bool r = columnRoot->Advance(database, timePath, 0, direction);
        std::reverse(timePath.begin(), timePath.end());
        return r;

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTable::RootAdvance() -- %s", err.what());
        throw err;
    }
}

void
MetaDataTableImp::RootGetColumns(const MetaKey& columnIn, std::vector<MetaKey>& columns)
{
    DEBUG_LOCAL("MetaDataTableImp::RootGetColumns %s\n",
                columnIn.GetLiteral().c_str());

    // translate root column to empty address-type MetaKey
    MetaKey column = columnIn;
    if (column.IsUnknown())
        column = MetaKey::EmptyAddress();

    MetaKey wild = column;
    wild.SetWildcard(true);

    MetaDataMapImp::iterator i = mMetaDataMap->lower_bound(column);
    MetaDataMapImp::iterator cend = mMetaDataMap->upper_bound(wild);
    for (; i != cend; i++)
    {
        columns.push_back(i->first);
    }
}

bool
MetaDataTableImp::HasColumn(const MetaKey& column) const
{
    return mMetaDataMap->find(column) != mMetaDataMap->end();
}

MetaDataTable
MetaDataTableImp::GetTable(const Database& database, const MetaKey& column, const MetaData& metaData) const
{
    TableAddress ta(column, metaData.mPath, metaData.mVersion);
    UtcTimeScheme timeScheme(database.GetPeriods());
    MetaDataTable sub = Cache::Get().Find(database, ta);

    return sub;
}

void
MetaDataTableImp::Commit(const DatabaseImp& database,
                         const MetaKey& column,
                         const TimePath& writtenLeaf,
                         const Version& leafVersion,
                         std::map<size_t, TouchedMetaDataMap>& touched)
{
    try {
        Context ctx("MetaDataTable::Commit %s, leaf %s", column.GetLiteral().c_str(), writtenLeaf.GetLiteral().c_str());
        CacheLocker cl(this);

        DEBUG_LOCAL("MetaDataTableImp::Commit layer %ld\n", mLayer);
        DEBUG_LOCAL("    committing leaf %s:%s\n", column.GetLiteral().c_str(), writtenLeaf.GetLiteral().c_str());

        // * This is always called on the newly replicated node
        // * Walk the tree of written nodes, updating leaf metadata versions
        //   and adding paths to visited metadata tables along with the
        //   incremented version number for that table to a set in the process
        // * Iterate the set and persist each table with the version

        MetaKey localKey;

        // DEFENSIVE
        if (mPersisted)
            throw cor::Exception("Commit called on persisted table\n");

        if (IsRoot())
            localKey = column;
        else
            localKey = writtenLeaf.at(LayerAddressed());

        MetaDataMapImp::iterator i = mMetaDataMap->find(localKey);

        if (i == mMetaDataMap->end())
        {
            // if this is the root, don't try to construct a TimePath to new entity;
            // it uses Address only
            nabu::TimePath newNodeTimePath;
            if (!IsRoot())
            {
                newNodeTimePath = writtenLeaf.RootDownTo(LayerAddressed());
            }
            if (!IsLeaf())
            {
                TableAddress ta(column, newNodeTimePath, Version());
                MetaDataTable newTable(new MetaDataTableImp(mTimeScheme,
                                                            ta,
                                                            LayerAddressed()));
                Cache::Get().Put(newTable);
            }

            (*mMetaDataMap)[localKey] = MetaData(newNodeTimePath, Version().mNumber);
            i = mMetaDataMap->find(localKey);
        }

        if (!IsLeaf())
        {
            MetaDataTable sub;
            sub = GetTable(database, column, i->second);

            // DEFENSIVE -- avoid infinite recursion
            if (sub.get() == this)
            {
                throw FatalException(database.GetUrl(),
                                     "MetaDataTableImp::Commit -- breaking infinite loop");
            }

            TableAddress subAddress = sub->GetTableAddress();

            TableAddress addressOnly = subAddress;
            addressOnly.Zero();
            if (touched[LayerAddressed()].find(addressOnly) == touched[LayerAddressed()].end())
            {
                TableAddress newVersion = subAddress;
                newVersion.NewVersion();
                VersionPair entry;
                entry.mOld = subAddress.GetVersion();
                entry.mNew = newVersion.GetVersion();
                touched[LayerAddressed()][addressOnly] = entry;

                DEBUG_LOCAL("Creating new table %s from old table %s\n",
                            newVersion.GetLiteral().c_str(), subAddress.GetLiteral().c_str());

                // create new table with next version, and put it in the cache
                MetaDataTable advanced(sub->Replicate()); // replicate a new one
                advanced->mTableAddress = newVersion;     // with a new version
                // and update the version in this container so that it will be
                // used hereafter
                i->second.mVersion = advanced->mTableAddress.GetVersion();
                sub = advanced;

                // Cache is disabled at this point (in RootCommit); this will be the
                // only copy of this table, to be potentially modified multiple times
                // by this Commit operation before finally being persisted in RootCommit
                Cache::Get().Put(advanced);
            }

            // 'sub' at this point refers to the new child table
            sub->Commit(database, column, writtenLeaf, leafVersion, touched);
        }
        else
        {
            i->second.mVersion = leafVersion;
        }

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTableImp::Commit() -- %s", err.what());
        throw err;
    }
}

size_t
MetaDataTableImp::Remove(const DatabaseImp& database,
                         const MetaKey& column,
                         const TimePath& erasedLeaf,
                         std::map<size_t, TouchedMetaDataMap>& touched,
                         std::set<Garbage::Entry>& removedFiles)
{
    try {
        Context ctx("MetaDataTable::Remove %s, leaf %s", column.GetLiteral().c_str(), erasedLeaf.GetLiteral().c_str());
        CacheLocker cl(this);

        DEBUG_LOCAL("Remove layer %ld\n", mLayer);

        size_t r = 0;

        MetaKey localKey;
        if (IsRoot())
            localKey = column;
        else
            localKey = erasedLeaf.at(LayerAddressed());

        MetaDataMapImp::iterator i = mMetaDataMap->find(localKey);

        if (i != mMetaDataMap->end())
        {
            if (IsLeaf())
            {
                TableAddress ta(column, erasedLeaf, i->second.mVersion);
                DEBUG_LOCAL("Putting data at %s (%s) in garbage\n", erasedLeaf.GetLiteral().c_str(), ta.GetLiteral().c_str());
                removedFiles.insert(Garbage::Entry(ta, FileType::eData));

                // DEFENSIVE; should exist
                if (mMetaDataMap->erase(localKey) != 1)
                {
                    throw cor::Exception("MetaDataTableImp::Remove() -- Erasure of leaf node '%s' had no entry in table %s",
                        localKey.GetLiteral().c_str(), mTableAddress.GetLiteral().c_str());
                }

                r++;
            }
            else
            {
                MetaDataTable sub;
                sub = GetTable(database, column, i->second);

                // DEFENSIVE -- avoid infinite recursion
                if (sub.get() == this)
                {
                    throw FatalException(database.GetUrl(),
                                         "MetaDataTableImp::Remove -- breaking infinite loop");
                }

                TableAddress subAddress = sub->GetTableAddress();
                TableAddress addressOnly = subAddress;
                addressOnly.Zero();

                // only increment version for removal once
                TouchedMetaDataMap& touchedThisLayer = touched[LayerAddressed()];
                if (touchedThisLayer.find(addressOnly) == touchedThisLayer.end())
                {
                    TableAddress newVersion = subAddress;
                    newVersion.NewVersion();
                    VersionPair entry;
                    entry.mOld = subAddress.GetVersion();
                    entry.mNew = newVersion.GetVersion();
                    touchedThisLayer[addressOnly] = entry;

                    DEBUG_LOCAL("Creating new table %s from old table %s\n",
                                newVersion.GetLiteral().c_str(), subAddress.GetLiteral().c_str());

                    // create new table with next version, and put it in the cache
                    MetaDataTable advanced(sub->Replicate()); // replicate a new one
                    advanced->mTableAddress = newVersion;     // with a new version
                    // and update the version in this container so that it will be
                    // used hereafter
                    i->second.mVersion = advanced->mTableAddress.GetVersion();
                    sub = advanced;

                    // Cache is disabled at this point (in RootCommit); this will be the
                    // only copy of this table, to be potentially modified multiple times
                    // before finally being persisted in RootCommit
                    Cache::Get().Put(advanced);
                }

                // 'sub' at this point refers to the new child table
                r += sub->Remove(database, column, erasedLeaf, touched, removedFiles);

                if (sub->Empty())
                {
                    Version version = i->second.mVersion;
                    mMetaDataMap->erase(localKey); // i is now invalid

                    //TableAddress ta(column, IsRoot() ? TimePath() : erasedLeaf.RootDownTo(LayerAddressed()), version);
                    TableAddress ta = sub->GetTableAddress();
                    //printf("Putting metadata %s in garbage\n", ta.GetLiteral().c_str());
                    removedFiles.insert(Garbage::Entry(ta, FileType::eMetaData));

                    // DEFENSIVE
                    if (touchedThisLayer.find(addressOnly) == touchedThisLayer.end())
                        throw cor::Exception("Remove -- table not found\n");

                    TableAddress oldTa = ta;
                    oldTa.SetVersion(touchedThisLayer[addressOnly].mOld);
                    //printf("Putting metadata %s in garbage\n", oldTa.GetLiteral().c_str());
                    removedFiles.insert(Garbage::Entry(oldTa, FileType::eMetaData));

                    // this metadata, now empty, might have been added to 'touched'
                    // already from a previous removal; so make sure it is no longer
                    // in this collection to avoid having it persisted
                    touchedThisLayer.erase(addressOnly);
                }
            }

        }
        else
        {
            // DEFENSIVE
            throw cor::Exception("Can't find local key %s for %s in table %s",
                                 localKey.GetLiteral().c_str(),
                                 erasedLeaf.GetLiteral().c_str(),
                                 GetTableAddress().GetLiteral().c_str());
        }

        return r;

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTableImp::Remove() -- %s", err.what());
        throw err;
    }
}

size_t
MetaDataTableImp::DeleteUnused(DatabaseImp& database)
{
    DEBUG_LOCAL("MetaDataTableImp::DeleteUnused() layer %ld\n", mLayer);

    size_t removed = 0;

    MetaDataMapImp::iterator i = mMetaDataMap->begin();
    for (; i != mMetaDataMap->end(); i++)
    {
        if (!IsLeaf())
        {
            //printf("Find -- descending\n");
            TableAddress ta(IsRoot() ? i->first : mTableAddress.GetColumn(), i->second.mPath, i->second.mVersion);
            // Directly access cache, rather than using GetTable, so that if the
            // table is already deleted, a new empty one will not be created
            MetaDataTable sub = Cache::Get().At(database, ta);
            if (!sub)
                continue; // table already deleted

            //printf("Checking sub %s\n", sub->GetLiteral().c_str());
            if (!database.VersionInUse(sub->mTableAddress))
            {
                //printf("   sub is not used, descending\n");
                removed += sub->DeleteUnused(database);
                //if (sub->Empty())
                {
                    //printf("table %s is empty, deleting it\n");
                    std::string s = database.GetFileNamer().NameOf(sub->mTableAddress, FileType::eMetaData);
                    //printf("deleting '%s'\n", s.c_str());
                    removed += cor::File::Delete(s);
                    // remove the table from the cache
                    Cache::Get().Remove(sub);
                }
            }
            else
            {
                //printf("   sub is in use, descending no further\n");
            }
        }
        else
        {
            TableAddress ta(mTableAddress.GetColumn(), i->second.mPath, i->second.mVersion);
            //printf("Checking leaf %s\n", ta.GetLiteral().c_str());
            if (!database.VersionInUse(ta))
            {
                //printf("   leaf is not used, deleting\n");
                std::string s = database.GetFileNamer().NameOf(ta, FileType::eData);
                //printf("deleting '%s'\n", s.c_str());
                removed += cor::File::Delete(s);
            }
            else
            {
                //printf("   leaf is in use\n");
            }
        }
    }
    return removed;
}

CopyStats
MetaDataTableImp::RootPullUniqueNodes(const Database& source,
                                      DatabaseImp& destination)
{
    DEBUG_LOCAL("MetaDataTableImp::RootPullUniqueNodes() remote '%s'\n",
                destination.GetUrl().GetLiteral().c_str());

    // DEFENSIVE
    if (!IsRoot())
    {
        throw cor::Exception("MetaDataTable::RootPullUniqueNodes -- table '%s' is not root", GetLiteral().c_str());
    }

    CopyStats copied;
    if (!destination.VersionInUse(GetTableAddress()))
    {
        copied += PullUniqueNodes(source, destination);

        destination.CopyFileFrom(source, GetTableAddress(), FileType::eMetaData);

        copied.mCopied++;
    }
    copied.mCompared++;

    return copied;
}

CopyStats
MetaDataTableImp::PullUniqueNodes(const Database& source,
                                  DatabaseImp& destination)
{
    DEBUG_LOCAL("MetaDataTableImp::PullUniqueNodes() layer %ld\n", mLayer);

    CopyStats copied;

    MetaDataMap diffs = MetaDataMap(new MetaDataMapImp());
    *diffs.get() = *mMetaDataMap.get();

    destination.GetUnusedVersions(mTableAddress, diffs);
    copied.mCompared++;

    MetaDataMapImp::iterator i = diffs->begin();
    for (; i != diffs->end(); i++)
    {
        if (!IsLeaf())
        {
            MetaDataTable sub = GetTable(source, IsRoot() ? i->first : mTableAddress.GetColumn(), i->second);
            if (!destination.VersionInUse(sub->mTableAddress))
            {
                //printf("   sub is missing in destination, descending\n");
                copied += sub->PullUniqueNodes(source, destination);

                destination.CopyFileFrom(source, sub->mTableAddress, FileType::eMetaData);
            }
        }
        else
        {
            TableAddress ta(mTableAddress.GetColumn(), i->second.mPath, i->second.mVersion);
            destination.CopyFileFrom(source, ta, FileType::eData);
        }
    }

    copied.mCopied += diffs->size();
    return copied;
}

CopyStats
MetaDataTableImp::RootPushUniqueNodes(const DatabaseImp& source,
                                      Database& destination)
{
    DEBUG_LOCAL("MetaDataTableImp::RootPushUniqueNodes() source '%s', destination '%s'\n",
                source.GetLiteral().c_str(), destination.GetLiteral().c_str());

    // DEFENSIVE
    if (!IsRoot())
    {
        throw cor::Exception("MetaDataTable::RootPushUniqueNodes -- table '%s' is not root", GetLiteral().c_str());
    }

    CopyStats copied;
    if (!destination.VersionInUse(GetTableAddress()))
    {
        copied += PushUniqueNodes(source, destination);

        source.CopyFileTo(destination, GetTableAddress(), FileType::eMetaData);

        copied.mCopied++;
    }
    copied.mCompared++;

    return copied;
}

CopyStats
MetaDataTableImp::PushUniqueNodes(const DatabaseImp& source,
                                  Database& destination)
{
    DEBUG_LOCAL("MetaDataTableImp::PushUniqueNodes() layer %ld\n", mLayer);

    CopyStats copied;

    MetaDataMap diffs = MetaDataMap(new MetaDataMapImp());
    *diffs.get() = *mMetaDataMap.get();

    destination.GetUnusedVersions(mTableAddress, diffs);
    copied.mCompared++;

    MetaDataMapImp::iterator i = diffs->begin();
    for (; i != diffs->end(); i++)
   {
        if (!IsLeaf())
        {
            MetaDataTable sub = GetTable(source, IsRoot() ? i->first : mTableAddress.GetColumn(), i->second);
            if (!destination.VersionInUse(sub->mTableAddress))
            {
                copied += sub->PushUniqueNodes(source, destination);
                source.CopyFileTo(destination, sub->mTableAddress, FileType::eMetaData);
            }
        }
        else
        {
            TableAddress ta(mTableAddress.GetColumn(), i->second.mPath, i->second.mVersion);
            destination.CopyFileFrom(source, ta, FileType::eData);
        }
    }

    copied.mCopied += diffs->size();

    return copied;
}

void
MetaDataTableImp::RootCountMissing(const DatabaseImp& database, AsyncStatus& stats)
{
    DEBUG_LOCAL("MetaDataTableImp::RootCountMissing()%s\n", "");

    // DEFENSIVE
    if (!IsRoot())
    {
        throw cor::Exception("MetaDataTable::RootCountMissing -- table '%s' is not root", GetLiteral().c_str());
    }

    std::string s = database.GetFileNamer().NameOf(GetTableAddress(), FileType::eMetaData);
    stats.ProcessedInc(1); // ++
    if (cor::File::Exists(s))
    {
        CountMissing(database, stats);
    }
    else
    {
        stats.CountInc(1); // ++
        DEBUG_LOCAL("MetaDataTableImp::RootCountMissing() -- missing root metadata '%s'\n", s.c_str());
    }

}

void
MetaDataTableImp::CountMissing(const DatabaseImp& database, AsyncStatus& stats)
{
    DEBUG_LOCAL("MetaDataTableImp::CountMissing() layer %ld\n", mLayer);

    MetaDataMapImp::iterator i = mMetaDataMap->begin();
    for (; i != mMetaDataMap->end(); i++)
    {
        if (!IsLeaf())
        {
            std::string s = database.GetFileNamer().NameOf(IsRoot() ? i->first : mTableAddress.GetColumn(),
                                                            i->second.mPath,
                                                            i->second.mVersion,
                                                            FileType::eMetaData);
            stats.ProcessedInc(1); // ++
            if (cor::File::Exists(s))
            {
                DEBUG_LOCAL("File '%s' exists\n", s.c_str());
                MetaDataTable sub = GetTable(database, IsRoot() ? i->first : mTableAddress.GetColumn(), i->second);

                // DEFENSIVE
                if (!sub)
                {
                    // this should be impossible unless the file is corrupted
                    throw cor::Exception("CountMissing: NULL table returned");
                }
                sub->CountMissing(database, stats);
            }
            else
            {
                printf("MetaDataTableImp::CountMissing() -- missing metadata '%s'\n", s.c_str());
                stats.CountInc(1); // ++
            }
        }
        else
        {
            TableAddress ta(mTableAddress.GetColumn(), i->second.mPath, i->second.mVersion);
            std::string s = database.GetFileNamer().NameOf(ta, FileType::eData);
            if (!cor::File::Exists(s))
            {
                printf("MetaDataTableImp::CountMissing() -- missing data '%s'\n", s.c_str());
                stats.CountInc(1); // ++
            }
            stats.ProcessedInc(1); // ++
        }
    }
}

bool
MetaDataTableImp::Find(const DatabaseImp& database,
                       TimePath& timePathReversed,
                       size_t index,
                       IterationDirection direction,
                       const TimePath& startIn) const
{
    CacheLocker cl(this);

    DEBUG_LOCAL("MetaDataTableImp::Find -- index %ld, timePathReversed %s, start %s\n",
           index,
           timePathReversed.GetLiteral().c_str(),
           startIn.GetLiteral().c_str());

    TimePath start = startIn;

    // DEFENSIVE
    if (mMetaDataMap->empty())
    {
        // this should never happen
        printf("MetaDataTableImp::Find() -- No metadata in table!!!!!!\n");
        return false;
    }

    Chunk chunk = start.at(LayerAddressed());
    MetaDataMapImp::iterator i;
    if (direction == eIterateForwards)
    {
        // find table entry greater or equal to chunk
        i = mMetaDataMap->lower_bound(chunk);
        if (i == mMetaDataMap->end())
        {
            //printf("no entry in %s >= %ld\n", GetLiteral().c_str(), chunk);
            return false;
        }
    }
    else
    {
        // find table entry less than or equal to chunk
        i = mMetaDataMap->upper_bound(chunk);
        if (i == mMetaDataMap->begin())
        {
            //printf("no entry <= %ld\n", chunk);
            return false;
        }
        // if i == end(), then i-- will point to final member of table
        i--;
    }
    //printf("first entry is at %d\n", i->first.GetChunk());

    Chunk first = i->first.GetChunk();

    // if the table entry chunk is greater than that of its timePath counterpart,
    // then a later time period is being considered, so zero out the remaining
    // components of timePath.
    // Ex.) if looking for June 13, 2020, and the first month in 2020 is August,
    // don't look for data from the 13th; look for data from the 1st. That is
    // the earliest data available after June 13 for that year.
    // Ex.) if the last month in 2020 is March, look for data from the 31st.
    // That is the latest data available before June 13 for that year.
    if (direction == eIterateForwards)
    {
        if (first > chunk)
        {
            start[LayerAddressed()] = first;
            // XXX use ZeroOut here
            for (size_t i = 0; i < LayerAddressed(); i++)
                start[i] = 0;
        }
    }
    else
    {
        if (first < chunk)
        {
            const std::vector<unsigned int>& periods = database.GetTimeScheme().GetPeriods();

            start[LayerAddressed()] = first;
            for (size_t i = 0; i < LayerAddressed(); i++)
                start[i] = periods[i + 1];
        }
    }


    //printf("%s: Pushing back %d\n", timePathReversed.GetLiteral().c_str(), first);
    timePathReversed.push_back(first);
    //printf("    %s\n", timePathReversed.GetLiteral().c_str());


    if (!IsLeaf())
    {
        //printf("Find -- descending\n");
        MetaDataTable sub = GetTable(database, mTableAddress.GetColumn(), i->second);
        // DEFENSIVE
#ifndef USE_INCREMENTING_NUMBERS
        if (sub->mTableAddress.GetVersion() == Version())
            throw cor::Exception("MetaDataTable::Find() -- tried to create new table at '%s'",
                                 mTableAddress.GetColumn().GetLiteral().c_str());
#endif
        bool r = sub->Find(database, timePathReversed, index + 1, direction, start);

        // if 'start' was not found, then return the next node in the appropriate
        // direction, if any
        if (direction == eIterateForwards)
        {
            i++;
            if (!r && i != mMetaDataMap->end())
            {
                //printf(">>> Miss (forward) for %s\n", start.GetLiteral().c_str());
                // zero out remainder
                size_t remainder = LayerAddressed() + 1;
                timePathReversed = start.ZeroOut(remainder);
                start[LayerAddressed()] = i->first.GetChunk();
                timePathReversed.push_back(i->first.GetChunk()); // replace path
                sub = GetTable(database, mTableAddress.GetColumn(), i->second);
                r = sub->Find(database, timePathReversed, index + 1, direction, start);
            }
        }
        else
        {
            if (!r && i != mMetaDataMap->begin())
            {
                i--;
                //printf(">>> Miss (backwards)\n");
                //printf("start = %s\n", start.GetLiteral().c_str());
                // zero out remainder
                size_t remainder = LayerAddressed() + 1;
                //printf("remainder = %ld\n", remainder);
                timePathReversed = start.ZeroOut(remainder);
                //printf("1 timePathReversed = %s\n", timePathReversed.GetLiteral().c_str());
                //printf("1 start = %s\n", start.GetLiteral().c_str());
                database.GetTimeScheme().SetToLast(start, remainder);
                //printf("2 timePathReversed = %s\n", timePathReversed.GetLiteral().c_str());
                start[LayerAddressed()] = i->first.GetChunk();
                //printf("2 start = %s\n", start.GetLiteral().c_str());
                timePathReversed.push_back(i->first.GetChunk()); // replace path
                //printf("3 timePathReversed = %s\n", timePathReversed.GetLiteral().c_str());
                sub = GetTable(database, mTableAddress.GetColumn(), i->second);
                r = sub->Find(database, timePathReversed, index + 1, direction, start);
            }
        }
        //printf("Find -- ascending with result %s\n", r ? "true" : "false");
        return r;
    }

    // DEFENSIVE
#ifndef USE_INCREMENTING_NUMBERS
    if (i->second.mVersion == Version().mNumber)
    {
        throw cor::Exception("MetaDataTableImp::Find() -- returning entry (%s) with no version", timePathReversed.GetLiteral().c_str());
    }
#endif

    // leaf behavior
    //printf("MetaDataTableImp::Find -- leaf behavior\n");
    return true;
}

bool
MetaDataTableImp::Advance(const DatabaseImp& database,
                          TimePath& timePath,
                          size_t index,
                          IterationDirection direction) const
{
    CacheLocker cl(this);

    DEBUG_LOCAL("MetaDataTableImp::Advance() %s, index %ld, timePath %s, direction %s\n",
                GetLiteral().c_str(),
                index,
                timePath.GetLiteral().c_str(),
                direction == eIterateBackwards ? "backwards" : "forwards");

    // this is when a node is entered for the first time;
    if (timePath.size() <= index)
    {
        if (mMetaDataMap->empty()) // table exists, but has no metadata entries
        {
            //printf("Empty table%s\n","");
            return false;
        }

        //printf("1: Pushing back %d\n", mMetaDataMap->begin()->first.GetChunk());
        if (direction == eIterateForwards)
            timePath.push_back(mMetaDataMap->begin()->first.GetChunk());
        else
            timePath.push_back(mMetaDataMap->rbegin()->first.GetChunk());

        if (IsLeaf())
            return true;
    }

    Chunk chunk = timePath[index];
    //printf("Finding chunk %d\n", chunk);
    MetaDataMapImp::const_iterator i;
    if (direction == eIterateForwards)
        i = mMetaDataMap->lower_bound(chunk);
    else
    {
        i = mMetaDataMap->upper_bound(chunk);
        i--;
    }

    if (i == mMetaDataMap->end())
    {
        //printf("i not found\n");
        return false;
    }

    if (!IsLeaf())
    {
        //printf("Advance -- this is a non-leaf (timePath size is %ld)\n", timePath.size());

        //while (i != mMetaDataMap->end()) // backwards iteration end condition handled in loop
        while(true)
        {
            timePath[index] = i->first.GetChunk();

            MetaData md = i->second;
            MetaDataTable sub = GetTable(database, mTableAddress.GetColumn(), md);

            // DEFENSIVE
#ifndef USE_INCREMENTING_NUMBERS
            if (sub->mTableAddress.GetVersion() == Version())
                throw cor::Exception("MetaDataTable::Advance() -- tried to create new table at '%s'",
                                     mTableAddress.GetColumn().GetLiteral().c_str());
#endif

            // DEFENSIVE -- avoid infinite recursion
            if (sub.get() == this)
            {
                throw FatalException(database.GetUrl(),
                                     "MetaDataTableImp::Advance() -- infinite recursion detected");
            }

            //printf("Advance -- %ld descending\n", index);
            if (sub->Advance(database, timePath, index + 1, direction))
            {
                //printf("Advance -- %ld ascending\n", index);
                return true;
            }
            //printf("Advance -- %ld missed, iterating %s\n", index, direction == eIterateForwards ? "forwards" : "backwards");

            // miss; advance to next item, if possible, or fail
            if (direction == eIterateForwards)
            {
                i++;
                if (i == mMetaDataMap->end())
                {
                    //printf("Hit end\n");
                    break;
                }
            }
            else
            {
                if (i == mMetaDataMap->begin())
                {
                    //printf("Hit beginning\n");
                    break;
                }
                i--;
            }
        }
        //printf("Advance -- Hit the end of non-leaf, popping\n");

        timePath.pop_back();
        return false;
    }

    //printf("Advance -- this is a leaf (timePath size is %ld)\n", timePath.size());
    // leaf behavior
    if (direction == eIterateForwards)
    {
        i++;
        if (i == mMetaDataMap->end())
        {
            //printf("End of leaf\n");
            timePath.pop_back();
            return false;
        }
    }
    else
    {
        if (i == mMetaDataMap->begin())
        {
            //printf("Beginning of leaf\n");
            timePath.pop_back();
            return false;
        }
        i--;
    }
    timePath[index] = i->first.GetChunk();
    // DEFENSIVE
#ifndef USE_INCREMENTING_NUMBERS
    if (i->second.mVersion == Version().mNumber)
    {
        throw cor::Exception("MetaDataTableImp::Advance() -- returning entry (%s) with no version", timePath.GetLiteral().c_str());
    }
#endif

    //printf("MetaDataTable::Advance() -- returning true\n");
    return true;
}

void
MetaDataTableImp::PrintAll(const DatabaseImp& database, std::ostream& os, unsigned indent)
{
    CacheLocker cl(this);

    std::string sindent(indent, ' ');

    os << sindent << "Table " << GetTableAddress().GetLiteral() << std::endl;

    //os << sindent << "Contents:";
    if (mMetaDataMap->empty())
        os << std::endl << sindent << "    empty" << std::endl;
    else
        os << std::endl;
    for (MetaDataMapImp::const_iterator i = mMetaDataMap->begin(); i != mMetaDataMap->end(); i++)
    {
        os << sindent << i->first.GetLiteral() << " is at version " << i->second.GetLiteral() << std::endl;
        if (!IsLeaf())
        {
            MetaDataTable sub = GetTable(database, IsRoot() ? i->first : mTableAddress.GetColumn(), i->second);
            //os << sindent << "Entry " << i->second.GetLiteral() << std::endl;

            // DEFENSIVE: avoid infinite recursion
            if (sub.get() == this)
            {
                throw FatalException(database.GetUrl(),
                                     "MetaDataTableImp::PrintAll() -- infinite recursion detected");
            }
            sub->PrintAll(database, os, indent + 4);
        }
        //else
        //    os << sindent << "    " << i->first.GetLiteral() << std::endl;
    }
}

size_t
MetaDataTableImp::TotalSize(const DatabaseImp& database)
{
    DEBUG_LOCAL("MetaDataTableImp::TotalSize() layer %ld\n", mLayer);
    CacheLocker cl(this);

    size_t count = 1; // one for this table itself

    if (IsLeaf())
    {
        count += mMetaDataMap->size();
        return count;
    }

    MetaDataMapImp::iterator i = mMetaDataMap->begin();
    for (; i != mMetaDataMap->end(); i++)
    {
        MetaDataTable sub = GetTable(database, IsRoot() ? i->first : mTableAddress.GetColumn(), i->second);

        // DEFENSIVE
        if (!sub)
        {
            throw FatalException(database.GetUrl(),
                                 "MetaDataTable::TotalSize() -- empty table returned");
        }

        count += sub->TotalSize(database);
    }
    return count;
}

void
MetaDataTableImp::Persist(const Database& database)
{
    DEBUG_LOCAL("MetaDataTableImp::Persist() -- %s\n", GetLiteral().c_str());
    Context ctx("MetaDataTable::Persist");
    // DEFENSIVE
    if (mPersisted)
        throw cor::Exception("Persist called on persisted table '%s'\n", GetLiteral().c_str());

    database.PersistMetadata(mMetaDataMap, mTableAddress);
    /*
    std::string fn = database.GetFileNamer().NameOf(mTableAddress, FileType::eMetaData);
    mMetaDataMap->Persist(fn);
    */

    mPersisted = true;
}

MetaDataTableImp*
MetaDataTableImp::Replicate()
{
    MetaDataTableImp* copy = new MetaDataTableImp(mTimeScheme, mTableAddress, mLayer);
    // do not just copy the smart pointer, allocate new table and copy contents
    copy->mMetaDataMap.reset(new MetaDataMapImp(mMetaDataMap->GetType()));
    *(copy->mMetaDataMap) = *mMetaDataMap;
    return copy;
}

size_t
MetaDataTableImp::GetNumberOfLayers()
{
    return mTimeScheme.Size();
}

bool
MetaDataTableImp::Load(const Database& database)
{

    const Version& version = mTableAddress.GetVersion();

    // DEFENSIVE
    if (!mMetaDataMap->empty())
        throw cor::Exception("Load of version %s called on non-empty table %s\n", version.GetLiteral().c_str(), GetLiteral().c_str());

    try {

        DEBUG_LOCAL("Metadata %s at version %s loading\n",
                    GetLiteral().c_str(),
                    mTableAddress.GetVersion().GetLiteral().c_str());

        if (!database.GetMetadata(mMetaDataMap, mTableAddress))
        {
            DEBUG_LOCAL("--------- Metadata %s : no file\n", mTableAddress.GetTimePath().GetLiteral().c_str());
            return false;
        }

        /*
        std::string fn = database.GetFileNamer().NameOf(mTableAddress, FileType::eMetaData);

        if (!cor::File::IsFile(fn))
        {
            DEBUG_LOCAL("-------------------- Metadata %s : no file '%s'\n", mTableAddress.GetTimePath().GetLiteral().c_str(), fn.c_str());
            return false;
        }

        mMetaDataMap->Load(fn);
         */

        // create the metadata table objects, as necessary
        MetaDataMapImp::iterator ci = mMetaDataMap->begin();

        for (; ci != mMetaDataMap->end(); ci++)
        {
            MetaKey newAddress = IsRoot() ? ci->first : mTableAddress.GetColumn();
            TimePath newPath = ci->first + mTableAddress.GetTimePath();

            DEBUG_LOCAL("MetaDataTableImp::Load -- creating new sub-table for column '%s' at key '%s', timepath '%s'\n",
                        newAddress.GetLiteral().c_str(), ci->first.GetLiteral().c_str(), newPath.GetLiteral().c_str());

            if (!IsRoot())
                ci->second.mPath = newPath;
            else
                ci->second.mPath = mTableAddress.GetTimePath();
            DEBUG_LOCAL("Setting path for new entry to %s\n", ci->second.mPath.GetLiteral().c_str());
        }

        DEBUG_LOCAL("Metadata %s loaded version %s\n",
                    mTableAddress.GetTimePath().GetLiteral().c_str(),
                    version.GetLiteral().c_str());

    } catch (cor::Exception& err)
    {
        err.SetWhat("MetaDataTable::Load() table '%s' : %s", GetLiteral().c_str(), err.what());
        throw err;
    }

    // SANITY
    if ((mTableAddress.GetVersion().Valid()) && (version.mNumber < mTableAddress.GetVersion().mNumber))
    {
        throw cor::Exception("version regression from %s to %s\n", mTableAddress.GetVersion().GetLiteral().c_str(), version.GetLiteral().c_str());
    }

    DEBUG_LOCAL("MetaDataTableImp::Load -- complete for %s\n", GetLiteral().c_str());
    return true;
}

void
MetaDataTableImp::LoadDocument(const std::string& document)
{
    // DEFENSIVE!
    if (Valid())
        throw cor::Exception("MetaDataTableImp::LoadDocument() -- table is already valid");

    Json::Value v;
    Json::Reader reader;
    if (!reader.parse(document, v))
        throw cor::Exception("MetaDataTableImp::LoadDocument() -- document is not JSON");

    LoadJson(v);
}

void
MetaDataTableImp::LoadJson(const Json::Value &root)
{
    mLayer = cor::ReadInt("layer", root);
    mTableAddress.ParseFromJson(root["table"]);
    mTimeScheme.FromJson(root["periods"]);

    mMetaDataMap.reset(new MetaDataMapImp(mTableAddress.IsRoot() ? MetaKey::eAddress : MetaKey::eChunk));
    mMetaDataMap->LoadDocument(root["metadata"].asString());
}

void
MetaDataTableImp::SaveJson(Json::Value &root) const
{

    root["layer"] = (unsigned int)mLayer;
    mTableAddress.RenderToJson(root["table"]);
    mTimeScheme.ToJson(root["periods"]);
    std::string s;
    mMetaDataMap->PersistDocument(s);
    root["metadata"] = s;
}

void
MetaDataTableImp::SaveDocument(std::string& document) const
{
    Json::Value v;
    SaveJson(v);
    Json::FastWriter writer;
    document = writer.write(v);
}

} //  end namespace
