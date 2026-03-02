//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_DATABASE_MAP_H
#define GICM_NABU_SERVER_DATABASE_MAP_H


#include <mcor/mmutex.h>
#include <nabu/database_imp.h>


namespace nabu {

enum DatabaseState { eOnline, eOffline };


/** Class DatabaseMap : a threadsafe collection of Nabu database implementations
 *  (that is, actual local databases).
 */
class DatabaseMap
{
public:
    DatabaseMap(const std::string& rootDirectory, int selectTimeoutSec);
    ~DatabaseMap();

    // load map file
    void Load();

    // deduce directories based on file paths
    void Deduce();

    // return the database at the given name, and its state; throws
    // an exception if not found
    DatabaseState Get(const std::string& name, DatabaseImpPtr& database);

    // returns true if database of the given name exists
    bool Has(const std::string& name) const;

    // return the names and states of the databases
    void GetDatabases(std::map<std::string, DatabaseState>& databases);

    // reverse lookup; sets key to first entry value of 'url'; returns
    // if any key exists
    bool FindByUrl(const cor::Url& url, std::string& key);

    // take the database online; ignored if it does not exist
    void GoOnline(const std::string& name);
    // take the database offline; ignored if it does not exist
    void GoOffline(const std::string& name);

    // add a database; throws if one already exists with that name
    void Add(const std::string& name, DatabaseImpPtr newDatabase);

    // remove a database; throws if not found
    void Delete(const std::string& name);

    std::string Print() const;

    struct Entry
    {
        std::string mName; // name in URL; not the directory
        DatabaseImpPtr mDatabase;
        DatabaseState mState;
    };
    typedef std::map<std::string, Entry> Imp;
    typedef Imp::const_iterator ConstIterator;
    typedef Imp::iterator Iterator;

    Iterator Begin(cor::ObjectLocker& ol);
    Iterator End();
    ConstIterator Begin(cor::ObjectLocker& ol) const;
    ConstIterator End() const;

private:
    const int mSelectTimeoutSec;
    mutable cor::MMutex mMutex;
    Imp mImp;

    const std::string mRootDirectory;

    // save the map of databases (not the databases themselves)
    void Save();

};


}

#endif
