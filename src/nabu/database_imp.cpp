//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <algorithm>
#include <errno.h>
#include <ftw.h>
#include <unistd.h>

#include <mcor/directory.h>
#include <mcor/mfile.h>
#include <mcor/mmutex.h>
#include <mcor/strutil.h>
#include <mcor/thread_analysis.h>

#include "access_imp.h"
#include "branch_imp.h"
#include "cache.h"
#include "config.h"
#include "exception.h"
#include "filenamer.h"
#include "garbage.h"
#include "database_imp.h"
#include "leak_detector.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu
{


DatabaseImp::DatabaseImp(const std::string& rootDirectory) :
    Database("file://127.0.0.1/" + rootDirectory),
    mFileNamer(rootDirectory),
    mGarbageCollector(*this),
    mJournalThread(*this),
    //mColumnInfoMutex("ColumnInfoMutex"),
    //mLabelFileMutex("LabelFileMutex"),
    mLabelFile(*this)
{
    DEBUG_LOCAL("DatabaseImp::DatabaseImp() -- root %s\n", rootDirectory.c_str());
    LeakDetector::Get().Increment("DatabaseImp");
}

DatabaseImp::~DatabaseImp()
{
    DEBUG_LOCAL("DatabaseImp destructor%s\n","");

    LeakDetector::Get().Decrement("DatabaseImp");
}


BranchImpPtr
DatabaseImp::GetBranchImp(const MetaKey& branchPath) const
{
    DEBUG_LOCAL("DatabaseImp::GetBranchImp() -- %s\n", branchPath.GetLiteral().c_str());
    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    // should now be in form, ["branch1", "branch2", "tag"], i.e., no leading "main"
    if (branchPath == MetaKey::MainBranch())
    {
        return mMainBranch;
    }

    BranchImpPtr r = mMainBranch->GetBranch(branchPath);
    if (r)
        return r;

    // miss
    return BranchImpPtr();
}

BranchImpPtr
DatabaseImp::GetMainImp()
{
    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    return mMainBranch;
}

BranchPtr
DatabaseImp::GetBranch(const MetaKey& branchPath)
{
    return GetBranchImp(branchPath);
}

BranchPtr
DatabaseImp::GetMain()
{
    return GetBranchImp(MetaKey::MainBranch());
}

BranchPtr
DatabaseImp::CreateBranch(const MetaKey& branchPath)
{
    return BranchPtr();
}

void
DatabaseImp::DeleteBranch(const MetaKey& branchPath)
{
    // XXX implement
}


void
DatabaseImp::CreateNew(const nabu::UtcTimeScheme& timeScheme)
{
    DEBUG_LOCAL("DatabaseImp::CreateNew() -- creating new database at root %s\n", mFileNamer.GetRoot().c_str());

    if (!TryCreateNew(timeScheme))
        throw cor::Exception("Database appears to already exist");
}

bool
DatabaseImp::TryCreateNew(const nabu::UtcTimeScheme &timeScheme)
{
    DEBUG_LOCAL("DatabaseImp::CreateNew() -- trying to create a new database at root %s\n",
                mFileNamer.GetRoot().c_str());

    try {
        Config cfg(mFileNamer.GetRoot());
        if (cfg.Exists())
            return false;

        mFingerprint = muuid::uuid::random();
        cfg.Create(timeScheme, mFingerprint);
    } catch (cor::Exception& err)
    {
        err.SetWhat("DatabaseImp::CreateNew -- %s", err.what());
        throw err;
    }

    return true;
}


CopyStats
DatabaseImp::Clone(const DatabasePtr remote)
{
    DEBUG_LOCAL("DatabaseImp::Clone() -- cloning new database at root directory %s from %s\n",
                mFileNamer.GetRoot().c_str(),
                remote->GetUrl().GetLiteral().c_str());

    CopyStats r;

    try {
        // create config file; this will throw if one exists, such as
        // if a database already exists at rootDirectory
        {
            Config cfg(mFileNamer.GetRoot());

            // get fingerprint
            muuid::uuid uuid;
            uuid.set(remote->GetFingerprint().string());

            // get time periods
            std::vector<unsigned int> timePeriods = remote->GetPeriods();
            UtcTimeScheme timeScheme(timePeriods);

            cfg.TryCreate(timeScheme, uuid);
        }
        Initialize();

        r = Pull(remote);

    } catch (cor::Exception& err)
    {
        err.SetWhat("DatabaseImp::Clone -- %s", err.what());
        throw err;
    }

    return r;
}

void
DatabaseImp::Initialize()
{
    DEBUG_LOCAL("DatabaseImp::Initialize%s\n", "");

    Config cfg(GetFileNamer().GetRoot());
    cfg.Parse();

    mTimeScheme = cfg.GetTimeScheme();
    mFingerprint = cfg.GetFingerprint();

    mMainBranch = BranchImpPtr(new BranchImp(*this, MetaKey::MainBranch()));

    // parse label description file, and determine the head of each
    // branch
    InitializeBranches();

    //InitializeColumns();
}

void
DatabaseImp::SetCacheLimit(int entries)
{
    DEBUG_LOCAL("DatabaseImp::SetCacheLimit() -- cache limit was %ld, now %d\n",
                Cache::Get().GetLimit(),
                entries);
    Cache::Get().SetLimit(entries);
}

bool
DatabaseImp::Exists()
{
    try {
        Config cfg(GetFileNamer().GetRoot());
        cfg.Parse();
        return true;
    }
    catch (...)
    {
        return false;
    }
}

AccessPtr
DatabaseImp::CreateAccess(const MetaKey& branchPath,
                             const cor::TimeRange& timeRangeIn,
                             AccessType type,
                             const std::string tagName)
{
    DEBUG_LOCAL("DatabaseImp::CreateAccess %s tag '%s'\n", timeRangeIn.Print().c_str(), tagName.c_str());

    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    AccessPtr newAccess;

    BranchImpPtr branch = GetBranchImp(branchPath);
    if (branch)
    {
        cor::TimeRange timeRange = timeRangeIn;

        // write access on a tag requires full time range
        if ((type == eWrite) && !tagName.empty())
        {
            timeRange = cor::TimeRange(cor::Time::NegativeInf(), cor::Time::PositiveInf());
        }
        // this will throw if tag does not exist
        try {
            newAccess.reset(new AccessImp(branch,
                                       tagName,
                                       muuid::uuid::random(),
                                       timeRange,
                                       mTimeScheme.Snap(timeRange),
                                       type));
            //printf("DatabaseImp::CreateAccess %s on tag '%s'\n", newAccess->GetHandle().string().c_str(), tagName.c_str());
        } catch (const cor::Exception& err)
        {
            // this exception can be expected when trying to manipulate a tag which may
            // or may not be in the branch. Testing for existence of the tag before gaining
            // an Access is non-atomic and unsafe. So this execution path is sort of the
            // trylock() on tags.
            printf("Could not create access at tag '%s': %s\n", tagName.c_str(), err.what());
        }
    }
    else
    {
        printf("Could not create access at branch '%s': branch not found\n", branchPath.GetLiteral().c_str());
    }

    return newAccess;
}

bool
DatabaseImp::VersionInUse(const TableAddress& nodeAddress)
{
    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    return mMainBranch->VersionInUse(nodeAddress);
}

void
DatabaseImp::GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs)
{
    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    return mMainBranch->GetUnusedVersions(ta, diffs);
}

void
DatabaseImp::WriteFile(const std::vector<unsigned char>& buffer,
                       const TableAddress& tableAddress,
                       FileType fileType)
{
    std::string destinationFile = GetFileNamer().NameOf(tableAddress, fileType);

    cor::File::MakePath(destinationFile);
    cor::File file(destinationFile, O_WRONLY | O_CREAT | O_TRUNC);
    file.WriteAll((const char*)buffer.data(), buffer.size());
}

void
DatabaseImp::ReadFile(std::vector<unsigned char>& data,
                      const TableAddress& tableAddress,
                      FileType fileType) const
{
    DEBUG_LOCAL("DatabaseImp::ReadFile() -- reading %s (%s)\n",
                tableAddress.GetLiteral().c_str(),
                FileTypeToString(fileType).c_str());
    std::string sourceFile = GetFileNamer().NameOf(tableAddress, fileType);

    cor::File file(sourceFile, O_RDONLY);
    data.resize(file.Size());
    file.Read((char*)data.data(), data.size());
}

bool
DatabaseImp::GetMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const
{
    std::string fn = GetFileNamer().NameOf(ta, FileType::eMetaData);

    if (!cor::File::IsFile(fn))
    {
        DEBUG_LOCAL("-------------------- Metadata %s : no file '%s'\n", ta.GetTimePath().GetLiteral().c_str(), fn.c_str());
        return false;
    }

    metaDataMap->Load(fn);
    return true;
}

void
DatabaseImp::PersistMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const
{
    std::string fn = GetFileNamer().NameOf(ta, FileType::eMetaData);

    metaDataMap->Persist(fn);
}

// nftw (Not Fully Thread Worthy?) does not have any private data scheme,
// so this thread local storage is used to allow multiple threads to use
// this callback at once. Set these pointers before invoking ntfw below.
// (reference: https://gcc.gnu.org/onlinedocs/gcc/Thread-Local.html)
static __thread DatabaseImp* nftwDataBase = NULL;
static __thread AsyncStatus* nftwData = NULL;

int
CountFileNode(const char *fp, const struct stat *info,
               const int typeflag, struct FTW *pathinfo)
{
    if (typeflag == FTW_F)
    {
        nftwData->TotalInc(1); // +=1
    }

    return 0;
}


int
HandleFileNode(const char *fp, const struct stat *info,
                const int typeflag, struct FTW *pathinfo)
{
    std::vector<std::string> v;
    std::string filePath(cor::File::CanonicalPath(fp, v));

    if (typeflag == FTW_F)
    {
        // turn filename into a node address
        TableAddress ta;
        FileType type;

        if (nftwDataBase->GetFileNamer().Parse(filePath, ta, type) && (type != FileType::eJournal))
        {
            //printf(" %s = %s (%s)\n", filePath.c_str(), ta.GetLiteral().c_str(), FileNamer::Extension(type).c_str());
            if (!nftwDataBase->VersionInUse(ta))
            {
                bool b = cor::File::Exists(filePath);
                if (b)
                {
                    printf("Orphan: %s\n", filePath.c_str());
                    cor::File::Delete(filePath);
                    nftwData->CountInc(1);
                }
            }
        }
        else
        {
            // this is expected for .nabu/instanc.cfg, etc.
            //printf(" %s is not a node file\n", fp);
        }
        nftwData->ProcessedInc(1); // += 1
    }
    else if (typeflag == FTW_D || typeflag == FTW_DNR || typeflag == FTW_DP)
    {
        int r = rmdir(filePath.c_str());
        if (r == 0)
        {
            //printf("Removed empty directory %s\n", filePath.c_str());
        }
        else
        {
            if ((errno != ENOTEMPTY) && (errno != EEXIST))
            {
                cor::ErrException err("Error deleting directory '%s' :", filePath.c_str());
                printf("%s\n", err.what());
            }
        }
    }

    return 0;
}

int
UpdateFileNode(const char *fp, const struct stat *info,
               const int typeflag, struct FTW *pathinfo)
{
    std::vector<std::string> v;
    std::string filePath(cor::File::CanonicalPath(fp, v));

    if (typeflag == FTW_F)
    {
        // turn filename into a node address
        TableAddress ta;
        FileType type;
        printf("%s\n", filePath.c_str());

        if (nftwDataBase->GetFileNamer().Parse(filePath, ta, type) && (type != FileType::eJournal))
        {
            std::string s = nftwDataBase->GetFileNamer().NameOf(ta, type);
            if (s != filePath)
            {
                cor::File source(filePath, O_RDWR);
                printf("    ->  %s\n", s.c_str());
                source.CopyTo(s);
                source.Delete();
                nftwData->CountInc(1);

                if (type == FileType::eMetaData)
                {
                    // XXX this assumes Chunk type files; roots done manually, or need
                    // to autodetect the type somehow
                    {
                        nabu::MetaDataMap meta(new nabu::MetaDataMapImp(nabu::MetaKey::eChunk));
                        try {
                            meta->LoadPermissive(s);
                            meta->Persist(s);
                        } catch (const cor::Exception& err)
                        {
                            printf("Could not update %s: %s\n", filePath.c_str(), err.what());
                        }
                    }
                }
            }
        }
        else
        {
            // this is expected for .nabu/instanc.cfg, etc.
            printf("    not a node file\n");
        }
        nftwData->ProcessedInc(1); // += 1
    }
    else
    {
        // directory, etc... don't care
        //if (typeflag == FTW_D || typeflag == FTW_DP)
        //    printf(" %s/\n", filePath.c_str());
    }

    return 0;
}

const int maxNumberOpenFds = 15;

/** class CleanOrphansThread
 *
 * asynchronously cleans all orphans
 *
 */
class CleanOrphansThread : public cor::Thread
{
public:
    CleanOrphansThread(DatabaseImp& database, AsyncStatus& orphanData) :
        cor::Thread("CleanOrphansThread"),
        mDataBase(database),
        mAsyncData(orphanData)
        {}
    ~CleanOrphansThread() {}

    void ThreadFunction()
    {
        nftwDataBase = &mDataBase;
        nftwData = &mAsyncData;

        try
        {

        // depth-first so that directories are emptied prior to trying to remove
        // the directory itself
        nftw(mDataBase.GetFileNamer().GetRoot().c_str(),
             HandleFileNode,
             maxNumberOpenFds,
             FTW_PHYS | FTW_DEPTH);

        } catch (const cor::Exception& err)
        {
            printf("\nError cleaning orphans: %s\n", err.what());
        }

        mAsyncData.SetRunning(false);
    }

    DatabaseImp& mDataBase;
    AsyncStatus& mAsyncData;
};

void
DatabaseImp::CleanOrphans()
{
    DEBUG_LOCAL("DatabaseImp::CleanOrphans%s\n","");

    if (mAsyncData.SetRunning(true))
    {
        throw cor::Exception("Operation is already in progress");
    }

    // DEFENSIVE
    if (!mMainBranch)
    {
        mAsyncData.SetRunning(false);
        throw cor::Exception("DatabaseImp is not initialized");
    }

    try {
        //mLabelFile.Load();

        // general algorithm:
        //   walk root directory
        //   parse each file into an address if possible, skip otherwise
        //   check the database to see if that address is in use
        //   delete the file if it is not

        // first, quickly count the number of files
        mAsyncData.Total(0);
        mAsyncData.Processed(0);
        nftwData = &mAsyncData;
        nftw(GetFileNamer().GetRoot().c_str(), CountFileNode, maxNumberOpenFds, FTW_PHYS);

        // now iterate all files and delete them
        mAsyncData.Count(0);
        CleanOrphansThread* coThread = new CleanOrphansThread(*this, mAsyncData);
        coThread->StartDetached();

    } catch (cor::Exception& err)
    {
        err.SetWhat("DatabaseImp::CleanOrphans() -- %s", err.what());
        mAsyncData.SetRunning(false);
        throw err;
    }
}

AsyncStatus
DatabaseImp::CleanOrphans(std::ostream &os)
{
    nabu::AsyncStatus status;
    CleanOrphans();
    while (true)
    {
        double p = PercentDoneAsynchronous(status);
        //fprintf(stderr, "\r%d%% of %ld", (int)(p), status.Total());
        os << "\r" << (int)p << "% of " << status.Total() << std::flush;

        if (p >= 100)
        {
            os << std::endl;
            break;
        }

        usleep(250000);
    }
    os << "Found " << status.Count() << " orphans in " << status.Total() << " total files" << std::endl;

    return status;
}

double
DatabaseImp::PercentDoneAsynchronous(AsyncStatus& stats)
{
    if (mAsyncData.Total() == 0)
        return 100;

    stats = mAsyncData;

    // DEFENSIVE -- avoid divide-by-zero
    if (stats.Total() == 0)
        return 100.0;

    return (double)stats.Processed() * 100.0 / (double)stats.Total();
}

/** class CountMissingThread
 *
 * asynchronously counts all missing files
 *
 */
class CountMissingThread : public cor::Thread
{
public:
    CountMissingThread(DatabaseImp& database) :
            cor::Thread("CountMissingThread"),
            mDataBase(database)
    {}
    ~CountMissingThread() {}

    void ThreadFunction()
    {
        try {
            mDataBase.DoCountMissing();
        }
        catch (const cor::Exception& err)
        {
            printf("Error counting missing files: %s\n", err.what());
        }
    }

    DatabaseImp& mDataBase;
};

void
DatabaseImp::CountMissing()
{
    if (mAsyncData.SetRunning(true))
    {
        throw cor::Exception("Operation is already in progress");
    }

    // DEFENSIVE
    if (!mMainBranch)
    {
        mAsyncData.SetRunning(false);
        throw cor::Exception("DatabaseImp is not initialized");
    }

    // zero out existing results
    mAsyncData.Processed(0);
    mAsyncData.Count(0);

    //mLabelFile.Load();

    // count the total nodes
    mAsyncData.Total(GetMain()->TotalSize());

    // spawn a thread to test all nodes
    CountMissingThread* thread = new CountMissingThread(*this);
    thread->StartDetached();
}

AsyncStatus
DatabaseImp::CountMissing(std::ostream &os)
{
    nabu::AsyncStatus status;
    CountMissing();
    while (true)
    {
        double p = PercentDoneAsynchronous(status);
        //fprintf(stderr, "\r%d%% of %ld", (int)(p), status.Total());
        os << "\r" << (int)p << "% of " << status.Total() << std::flush;

        if (p >= 100)
        {
            os << std::endl;
            break;
        }

        usleep(250000);
    }
    os << "Found " << status.Count() << " missing files in " << status.Total() << " total files" << std::endl;

    return status;
}

void
DatabaseImp::DoCountMissing()
{
    DEBUG_LOCAL("DatabaseImp::DoCountMissing() -- %s\n", GetFileNamer().GetRoot().c_str());

    try
    {
        //mLabelFile.Load();

        // detect brand-new database, which should have no files
        //
        // Note this is also the case if the root has been lost; there is
        // no way to tell the two cases apart. With the root gone, the rest
        // of the database looks like orphans with a new database, and there
        // is no way to prove this is actually not the case without using
        // history.
        /*if (GetMainImp()->GetHead()->GetTableAddress().IsBrandNewRoot())
        {
            mAsyncData.ProcessedInc(1); // += 1
            mAsyncData.SetRunning(false);
            return;
        }*/

        /*
        // iterator holds branch mutex
        BranchImp::Iterator i(mMainBranch);
        for (; i; i++)
        {
            found += (*i)->CountMissing();
        }
         */
        mMainBranch->CountMissing(mAsyncData);
        mAsyncData.SetRunning(false);

    } catch (cor::Exception& err)
    {
        err.SetWhat("Error initializing branches: %s\n", err.what());
        mAsyncData.SetRunning(false);
        throw err;
    }

}


/** class UpdateFilesThread
 *
 *  asynchronously updates filenames of all files
 *
 */
class UpdateFilesThread : public cor::Thread
{
public:
    UpdateFilesThread(DatabaseImp& database, AsyncStatus& orphanData) :
            cor::Thread("UpdateFilesThread"),
            mDataBase(database),
            mAsyncData(orphanData)
    {}
    ~UpdateFilesThread() {}

    void ThreadFunction()
    {
        nftwDataBase = &mDataBase;
        nftwData = &mAsyncData;

        nftw(mDataBase.GetFileNamer().GetRoot().c_str(), UpdateFileNode, maxNumberOpenFds, FTW_PHYS);

        mAsyncData.SetRunning(false);
    }

    DatabaseImp& mDataBase;
    AsyncStatus& mAsyncData;
};


void
DatabaseImp::UpdateFiles()
{
    if (mAsyncData.SetRunning(true))
    {
        throw cor::Exception("Operation is already in progress");
    }

    // DEFENSIVE
    if (!mMainBranch)
    {
        mAsyncData.SetRunning(false);
        throw cor::Exception("DatabaseImp is not initialized");
    }

    // zero out existing results
    mAsyncData.Processed(0);
    mAsyncData.Count(0);

    // first, quickly count the number of files
    mAsyncData.Total(0);
    mAsyncData.Processed(0);
    nftwData = &mAsyncData;
    nftw(GetFileNamer().GetRoot().c_str(), CountFileNode, maxNumberOpenFds, FTW_PHYS);

    // now iterate all files and delete them
    mAsyncData.Count(0);
    mAsyncData.Processed(0);

    // spawn a thread to test all nodes
    UpdateFilesThread* thread = new UpdateFilesThread(*this, mAsyncData);
    thread->StartDetached();
}

TimePath
DatabaseImp::GetPath(const cor::Time& in)
{
    return mTimeScheme.GetPath(in);
}

cor::TimeRange
DatabaseImp::GetTime(const TimePath& in)
{
    return mTimeScheme.GetTime(in);
}


void
DatabaseImp::FlushJournals()
{
    //DEBUG_LOCAL("DatabaseImp::FlushJournals()%s\n","");

    // ignore if database is not initialized
    if (!mMainBranch)
        return;

    /*
    // iterator holds branch mutex
    BranchImp::Iterator i(mMainBranch);
    for (; i; i++)
        (*i)->FlushJournals();
        */
    mMainBranch->FlushJournals();
}

void
DatabaseImp::PrintLabels(std::ostream& os)
{
    DEBUG_LOCAL("DatabaseImp::PrintLabels()%s\n", "");

    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");
    /*
    // iterator holds branch mutex
    BranchImp::Iterator i(mMainBranch);
    for (; i; i++)
        (*i)->PrintLabels(os, 0);
        */
    mMainBranch->PrintLabels(os, 0);
}

bool
DatabaseImp::TestCompareLabels(DatabaseImp& other)
{
    return mMainBranch->TestCompareLabels(other.mMainBranch);
}

size_t
DatabaseImp::DeleteRoot(const TableAddress& rootAddress)
{
    DEBUG_LOCAL("DatabaseImp::DeleteRoot() %s\n", rootAddress.GetLiteral().c_str());

    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    if (!rootAddress.IsRoot())
    {
        throw cor::Exception("DatabaseImp::DeleteRoot() -- '%s' is not a root\n", rootAddress.GetLiteral().c_str());
    }

    // check the root itself
    if (VersionInUse(rootAddress))
        return 0;

    MetaDataTable root = Cache::Get().At(*this, rootAddress);

    size_t r = 0;
    if (root)
    {
        r += root->DeleteUnused(*this);
        std::string s = mFileNamer.NameOf(root->GetTableAddress(), FileType::eMetaData);
        //printf("deleting '%s'\n", s.c_str());
        r += cor::File::Delete(s);
        Cache::Get().Remove(root);
    }
    return r;
}


CopyStats
DatabaseImp::Pull(const DatabasePtr remote)
{
    DEBUG_LOCAL("DatabaseImp::Pull() -- pulling %s from %s\n",
                GetLiteral().c_str(),
                remote->GetUrl().GetLiteral().c_str());

    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    // check the fingerprint on destination to verify it is related
    if (remote->GetFingerprint() != GetFingerprint())
    {
        throw cor::Exception("DatabaseImp::Pull() -- The remote database at %s is not related to %s",
                             remote->GetUrl().GetLiteral().c_str(),
                             GetLiteral().c_str());
    }

    CopyStats n;
    {
        cor::ContextScope cs("BranchPull");
        //n = mMainBranch->Pull(remote->GetBranch(nabu::MetaKey::MainBranch()));
        n = BranchImp::Sync(remote->GetBranch(nabu::MetaKey::MainBranch()), mMainBranch, BranchImp::ePull);
    }

    // this removes any labels (tag or branch) that exist here, but
    // not in source
    {
        cor::ContextScope cs("TrimLabels");
        TrimLabels(*remote);
    }

    return n;
}

CopyStats
DatabaseImp::Push(const DatabasePtr remote)
{
    DEBUG_LOCAL("DatabaseImp::Push() -- pushing %s to %s\n",
                GetLiteral().c_str(),
                remote->GetUrl().GetLiteral().c_str());

    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    // check the fingerprint on destination to verify it is related
    if (remote->GetFingerprint() != GetFingerprint())
    {
        throw cor::Exception("DatabaseImp::Pull() -- The remote database at %s is not related to %s",
                             remote->GetUrl().GetLiteral().c_str(),
                             GetLiteral().c_str());
    }

    CopyStats n;
    {
        cor::ContextScope cs("BranchPush");
        //n = mMainBranch->Push(remote->GetBranch(nabu::MetaKey::MainBranch()));
        n = BranchImp::Sync(mMainBranch, remote->GetBranch(nabu::MetaKey::MainBranch()), BranchImp::ePush);
    }

    // this removes any labels (tag or branch) that exist here, but
    // not in source
    {
        cor::ContextScope cs("TrimLabels");
        remote->TrimLabels(*this);
    }

    return n;
}

void
DatabaseImp::PersistTags()
{
    DEBUG_LOCAL("DatabaseImp::PersistTags()%s\n", "");

    try
    {
        //cor::ObjectLocker ol(mLabelFileMutex);
        //mLabelFile.Save();
    } catch (cor::Exception& err)
    {
        err.SetWhat("Error persisting tags to label file: %s\n", err.what());
        throw err;
    }
}

void
DatabaseImp::InitializeBranches()
{
    printf("DatabaseImp::InitializeBranches()%s\n", "");

    try
    {
        //mLabelFile.Load();
        std::string glob = mFileNamer.GetRoot() + "/branches/*.head";
        std::vector<std::string> candidateFiles;

        cor::File::FindFilenames(glob, candidateFiles);
        DEBUG_LOCAL("LabelFile::Load() -- Found %ld candidates from search '%s'\n",
                    candidateFiles.size(),
                    glob.c_str());

        // get version of each branch
        std::map<std::string, Version> versions;
        std::vector<std::string>::iterator ri = candidateFiles.begin();
        while (ri != candidateFiles.end())
        {
            try {
                std::string actualFile = cor::File::FollowSymlink(*ri);

                // parse branch name from symlink
                std::string::size_type lastSlash = ri->find_last_of('/');
                if (lastSlash == std::string::npos)
                {
                    printf("Malformed branch symlink: %s\n", ri->c_str());
                    continue;
                }
                std::string::size_type lastDot = ri->find_last_of('.');
                if ((lastDot == std::string::npos) || (lastDot < lastSlash))
                {
                    printf("Malformed branch symlink: %s\n", ri->c_str());
                    continue;
                }
                std::string branchName = ri->substr(lastSlash + 1, lastDot - lastSlash - 1);
                versions[branchName] = FileNamer::ExtractVersion(actualFile);
                printf("Found branch '%s' at version '%s'\n", branchName.c_str(), versions[branchName].GetLiteral().c_str());

                // XXX main branch should be an item in the branch collection, not its own class member
                if (branchName == "main")
                    mMainBranch->Initialize(versions[branchName]);
            }
            catch(const cor::Exception& err)
            {
                DEBUG_LOCAL("Database::InitializeBranches() -- could not extract version for file %s : %s\n", ri->c_str(), err.what());
                ri = candidateFiles.erase(ri);
                continue;
            }

            ri++;
        }

        if (candidateFiles.empty())
        {
            printf("Database::InitializeBranches() -- No valid branches found\n");
        }


        // branches are now known; determine head for each and flush their journals
        DEBUG_LOCAL("DatabaseImp::InitializeBranches() -- Initializing all branches%s\n", "");
    } catch (cor::Exception& err)
    {
        err.SetWhat("Error initializing branches: %s\n", err.what());
        throw err;
    }
}

/*void
DatabaseImp::InitializeColumns()
{
    DEBUG_LOCAL("DatabaseImp::InitializeColumns()%s\n", "");

    try
    {
        mColumnInfoFile.Load();

        if (mColumnInfo.empty())
        {
            printf("------------------------------------------------\n");
            printf("No column info found, inferring from column data\n");

            std::vector<MetaKey> columns;
            GetMain()->GetColumns(MetaKey(), columns);

            RecordType candidateTypes[] = { eTRR, ePITR };
            bool candidateSearchable[] = {true, false};

            for (size_t i = 0; i < columns.size(); i++)
            {
                std::string probeFileName = GetFileNamer().FindADataFile(columns[i]);

                if (!probeFileName.empty())
                {
                    ColumnInfo candidateInfo;
                    candidateInfo.mPeriods = GetTimeScheme().GetPeriods();

                    // try each combination of column type and searchability until
                    // the data file can be read successfully
                    size_t tI = 0;
                    for (; tI < sizeof(candidateTypes); tI++)
                    {
                        size_t sI = 0;
                        for (; sI < sizeof(candidateSearchable); sI++)
                        {
                            candidateInfo.mRecordType = candidateTypes[tI];
                            candidateInfo.mSearchable =  candidateSearchable[sI];

                            DataFile probeFile(probeFileName);
                            try
                            {
                                RecordVector r;
                                probeFile.Read(r, candidateInfo, eIterateForwards);
                                // if that worked, then candidateInfo is correct
                                break;
                            }
                            catch (const cor::Exception& err)
                            {
                            }
                        }
                        if (sI != sizeof(candidateSearchable))
                            break;
                    }

                    if (tI == sizeof(candidateTypes))
                    {
                        throw cor::Exception("Could not deduce colunn type for '%s'",
                                             columns[i].GetLiteral().c_str());
                    }

                    printf("%s is %s\n", columns[i].GetLiteral().c_str(), candidateInfo.GetLiteral().c_str());
                    mColumnInfo[columns[i]] = candidateInfo;
                }
            }

            // save this to create a column file
            Json::Value root;
            mColumnInfo.Render(root);
            mColumnInfoFile.Save(root);
        }

    } catch (cor::Exception& err)
    {
        err.SetWhat("Error initializing columns: %s\n", err.what());
        throw err;
    }
}

void
DatabaseImp::ParseColumns(const Json::Value& v)
{
    cor::ObjectLocker ol(mColumnInfoMutex);
    mColumnInfo.Parse(v);
}

ColumnInfo
DatabaseImp::GetColumnInfo(const MetaKey& column) const
{
    cor::ObjectLocker ol(mColumnInfoMutex);
    ColumnInfoMap::const_iterator i = mColumnInfo.find(column);
    if (i == mColumnInfo.end())
        throw cor::Exception("DatabaseImp::GetColumnInfo() -- Could not find column '%s'", column.GetLiteral().c_str());

    return i->second;
}

void
DatabaseImp::AddColumn(const MetaKey& column, const ColumnInfo& columnInfo)
{
    DEBUG_LOCAL("Database::AddColumn %s %s\n", column.GetLiteral().c_str(), columnInfo.GetLiteral().c_str());

    cor::ObjectLocker ol(mColumnInfoMutex);

    // test for conflict
    ColumnInfoMap::const_iterator i = mColumnInfo.find(column);
    if (i != mColumnInfo.end() && (i->second != columnInfo))
    {
        throw cor::Exception("DatabaseImp::AddColumn() -- Column '%s' (%s) already exists with different column info (%s)",
                             column.GetLiteral().c_str(),
                             columnInfo.GetLiteral().c_str(),
                             i->second.GetLiteral().c_str());
    }

    // if this is a new column
    if (i == mColumnInfo.end())
    {
        mColumnInfo[column] = columnInfo;

        // save column file
        Json::Value root;
        mColumnInfo.Render(root);
        mColumnInfoFile.Save(root);
    }
}

void
DatabaseImp::DeleteColumn(const MetaKey& column)
{
    DEBUG_LOCAL("Database::DeleteColumn %s\n", column.GetLiteral().c_str());

    cor::ObjectLocker ol(mColumnInfoMutex);

    ColumnInfoMap::iterator i = mColumnInfo.find(column);

    if (i != mColumnInfo.end())
    {
        // delete the data
        GetMain()->DeleteColumn(column);

        // delete the column info
        mColumnInfo.erase(i);

        // save column file
        Json::Value root;
        mColumnInfo.Render(root);
        mColumnInfoFile.Save(root);
    }
}

std::string
DatabaseImp::PrintColumns() const
{
    std::ostringstream oss;
    cor::ObjectLocker ol(mColumnInfoMutex);
    ColumnInfoMap::const_iterator i = mColumnInfo.begin();
    oss << mColumnInfo.size() << " columns:" << std::endl;
    for (; i != mColumnInfo.end(); i++)
    {
        oss << "   " << i->first << " : " << i->second.GetLiteral() << std::endl;
    }

    return oss.str();
}*/

TagImpPtr
DatabaseImp::GetTagImp(TagPtr tag)
{
    BranchImpPtr b = GetBranchImp(tag->GetBranch().GetBranchPath());
    return b->GetTagImp(tag);
}

} //  end namespace
