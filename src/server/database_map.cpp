//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/json_safe_file.h>
#include <mcor/strutil.h>
#include <mcor/url.h>
#include <json/value.h>

#include "database.h"
#include "database_map.h"

namespace nabu {

DatabaseMap::DatabaseMap(const std::string& rootDirectory, int selectTimeoutSec) :
    mSelectTimeoutSec(selectTimeoutSec),
    mMutex("DatabaseMap"),
    mRootDirectory(rootDirectory)
{
}

DatabaseMap::~DatabaseMap()
{
}

void
DatabaseMap::Load()
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Load");

    Json::SafeJsonFile file(mRootDirectory + "/publish.*.json");

    if (!file.Load())
        throw cor::Exception("DatabaseMap::Load() -- no publish file loadable at %s\n", mRootDirectory.c_str());

    Json::ValueIterator i = file.mRoot.begin();
    for (; i != file.mRoot.end(); i++)
    {
        std::string key = i.key().asString();
        std::string path = (*i).asString();

        DatabaseImpPtr db(new nabu::server::Database(path, mSelectTimeoutSec));
        Add(key, db);
    }
}

void
DatabaseMap::Deduce()
{
    // find all nabu directories under rootDirectory
    std::vector<std::string> v;
    cor::File::FindFilenames(mRootDirectory + "/*/.nabu/instance.cfg", v);
    if (v.empty())
        cor::File::FindFilenames(mRootDirectory + "/.nabu/instance.cfg", v);

    for (size_t i = 0; i < v.size(); i++)
    {
        std::vector<std::string> path;
        cor::File::CanonicalPath(v[0], path);
        path.pop_back(); // remove ".instance.cfg"
        path.pop_back(); // remove ".nabu"
        std::string rootPath = "/" + cor::Join(path, '/');
        // SANITY
        if (path.size() < 2)
            throw cor::Exception("DatabaseMap::Discover() -- path '%s' too short", v[0].c_str());

        try
        {
            Json::Reader reader;
            std::ifstream ifs(v[i]);
            Json::Value root;
            if (!reader.parse(ifs, root))
            {
                printf("Skipping %s due to bad 'instance.cfg': %s\n",
                       rootPath.c_str(),
                       reader.getFormattedErrorMessages().c_str());
                continue;
            }

            // default key is last part of path name, unless specified otherwise
            std::string key = path[path.size() - 1];
            DatabaseImpPtr db(new nabu::server::Database(rootPath, mSelectTimeoutSec));
            Add(key, db);
        }
        catch (const cor::Exception& err)
        {
            printf("Error adding discovered database at '%s': %s\n", rootPath.c_str(), err.what());
        }
    }
}

void
DatabaseMap::Save()
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Save");

    Json::SafeJsonFile file(mRootDirectory + "/publish.*.json");
    file.Load(); // discard contents; just needs to find next version to write

    Imp::const_iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        cor::Url url(i->second.mDatabase->GetUrl());
        file.mRoot[i->first] = "/" + url.GetResource();
    }

    file.Save();
}

DatabaseState
DatabaseMap::Get(const std::string& name, DatabaseImpPtr& database)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Get");
    Imp::iterator i = mImp.find(name);
    if (i == mImp.end())
        throw cor::Exception("No such database '%s'", name.c_str());

    database = i->second.mDatabase;
    return i->second.mState;
}

bool
DatabaseMap::Has(const std::string& name) const
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Get");
    return mImp.find(name) != mImp.end();
}

void
DatabaseMap::GetDatabases(std::map<std::string, DatabaseState>& databases)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::GetDatabases");

    Imp::iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        databases[i->first] = i->second.mState;
    }
}

bool
DatabaseMap::FindByUrl(const cor::Url& url, std::string& key)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::FindByUrl");

    Imp::iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        if (i->second.mDatabase->GetUrl() == url)
        {
            key = i->first;
            return true;
        }
    }
    return false;
}

void
DatabaseMap::GoOnline(const std::string& name)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::GoOnline");
    Imp::iterator i = mImp.find(name);
    if (i != mImp.end())
        i->second.mState = eOnline;

}

void
DatabaseMap::GoOffline(const std::string& name)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::GoOffline");
    Imp::iterator i = mImp.find(name);
    if (i != mImp.end())
        i->second.mState = eOffline;
}

void
DatabaseMap::Add(const std::string& name, DatabaseImpPtr newDatabase)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Add");

    Entry e;
    e.mState = eOnline;
    e.mDatabase = newDatabase;
    mImp[name] = e;

    Save();
}

void
DatabaseMap::Delete(const std::string& name)
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Delete");

    Imp::iterator i = mImp.find(name);
    if (i == mImp.end())
        throw cor::Exception("No such database '%s'", name.c_str());
    mImp.erase(i);

    Save();
}

std::string
DatabaseMap::Print() const
{
    cor::ObjectLocker ol(mMutex, "DatabaseMap::Print");

    std::string r;

    Imp::const_iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        r += i->first + " : " + std::string(i->second.mState == eOnline ? "(online)" : "OFFLINE");
    }

    return r;
}

DatabaseMap::Iterator
DatabaseMap::Begin(cor::ObjectLocker& ol)
{
    ol.Subsume(mMutex);
    return mImp.begin();
}

DatabaseMap::Iterator
DatabaseMap::End()
{
    return mImp.end();
}

DatabaseMap::ConstIterator
DatabaseMap::Begin(cor::ObjectLocker& ol) const
{
    ol.Subsume(mMutex);
    return mImp.begin();
}

DatabaseMap::ConstIterator
DatabaseMap::End() const
{
    return mImp.end();
}

}
