//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/url.h>
#include <mcor/strutil.h>
#include <nabu_client/database.h>

#include "backup.h"



namespace nabu {

BackupThread::BackupThread(nabu::DatabaseImp& database,
                           const std::string& destinationPath,
                           double hourPeriod) :
    cor::WallClockThread<int>("BackupThread"),
    mDataBase(database),
    mDestinationPath(destinationPath),
    mHourPeriod(hourPeriod)
{
    const int unusedItem = 0;
    cor::Time next; // now
    next += mHourPeriod * cor::Time::OneHour();
    next = next.TopOfMinute(cor::Time::eForwards, 0); // top of next minute
    PutItem(next, unusedItem);
}

BackupThread::~BackupThread()
{
    Stop();
}

void
BackupThread::HandleItem(const cor::Time& scheduled, const int& itemIgnored)
{
    // XXX hard-coded timezone
    const std::string timezone = "America/Denver";
    printf("\nSyncing database at %s %s local time\n", cor::LocalTime(timezone).Print().c_str(), timezone.c_str());

    // put the names of all destination databases to sync to into this container
    std::vector<std::string> destinations;

    cor::Time now;

    // start of month
    if (now.DayOfMonth() == 1)
        destinations.push_back(mDestinationPath + "/StartOfMonth");

    // start of weeks
    if (now.DayOfWeek() == 0)
    {
        int week = (now.DayOfMonth() / 7) + 1;
        std::ostringstream oss;
        oss << mDestinationPath << "/StartOfWeek" << week;
        destinations.push_back(oss.str());
    }

    // day of week, except for Sundays
    if (now.DayOfWeek() != 0)
    {
        std::string dow;
        now.DayOfWeek(dow);
        destinations.push_back(mDestinationPath + "/" + dow);
    }

    for (size_t i = 0; i < destinations.size(); i++)
    {
        printf("Remote (destination) is %s\n", destinations[i].c_str());
        DatabaseImp remote(destinations[i]);
        try {
            if (!remote.Exists())
            {
                remote.Clone(mDataBase.GetFingerprint().string(), mDataBase.GetPeriods());
            }
            remote.Initialize();
        } catch (const cor::Exception& err)
        {
            printf("Needed to create a brand-new destination database and could not: %s\n", err.what());
            continue;
        }

        try {
            printf("Cleaning orphans in remote...\n");
            remote.CleanOrphans();
            AsyncStatus localStats, remoteStats;
            while (true)
            {
                size_t proc;
                double pr = remote.PercentDoneAsynchronous(remoteStats);
                fprintf(stderr, "\rremote: %02d%% of %ld",
                        (int)(pr), remoteStats.Total());

                if (remoteStats.Done())
                    break;

                usleep(250000);
            }
            printf("\nremote: %ld processed, %ld orphans removed\n",
                   remoteStats.Processed(), remoteStats.Count());

            printf("Checking for missing files...\n");
            mDataBase.CountMissing();
            remote.CountMissing();
            while (true)
            {
                size_t proc;
                double pl = mDataBase.PercentDoneAsynchronous(localStats);
                double pr = remote.PercentDoneAsynchronous(remoteStats);
                fprintf(stderr, "\rlocal: %02d%% of %ld, remote: %02d%% of %ld",
                        (int)(pl), localStats.Total(),
                        (int)(pr), remoteStats.Total());

                if (localStats.Done() && remoteStats.Done())
                    break;

                usleep(250000);
            }
            printf("\nlocal: %ld processed, %ld missing\n",
                   localStats.Processed(), localStats.Count());
            printf("remote: %ld processed, %ld missing\n",
                   remoteStats.Processed(), remoteStats.Count());

            if (localStats.Count() != 0)
            {
                printf("!!!! Aborting: source database %s is corrupted, missing at least %ld files\n",
                       mDataBase.GetLiteral().c_str(),
                       localStats.Count());
            }
            if (remoteStats.Count() != 0)
            {
                printf("!!!! Aborting: remote database %s is corrupted, missing at least %ld files\n",
                       destinations[i].c_str(),
                       remoteStats.Count());
            }
            if ((localStats.Count() != 0) || (remoteStats.Count() != 0))
                continue;
            
        } catch (const cor::Exception& err)
        {
            printf("Error validating databases: %s\n", err.what());
            continue;
        }

        try
        {
            printf("Syncing...\n");
            nabu::CopyStats n = mDataBase.Sync(remote);

            // empty the garbage of remote, so that no orphans are left
            remote.GetGarbage().Start();
            remote.GetGarbage().WaitUntilEmpty();
            remote.GetGarbage().Stop();

            printf("Sync complete: %ld files moved, %ld nodes compared\n",
                   n.mCopied,
                   n.mCompared);
        } catch (const cor::Exception& err)
        {
            printf("!!!! Error syncing databases: %s->%s: %s\n",
                   mDataBase.GetLiteral().c_str(),
                   destinations[i].c_str(),
                   err.what());
        }
    }

    const int unusedItem = 0;
    cor::Time next;
    next += mHourPeriod * cor::Time::OneHour().Seconds();
    next = next.TopOfMinute(cor::Time::eForwards, 0); // next top of minute
    cor::LocalTime lt(timezone, next);
    printf("Next backup at %s %s local time\n", lt.Print().c_str(), timezone.c_str());
    PutItemReentrant(next, unusedItem);
}


/* SimpleBackupThread Implementation
 *
 */

SimpleBackupThread::SimpleBackupThread(nabu::DatabaseImp& database,
                           const std::string& destinationPath,
                           double hourPeriod) :
        cor::WallClockThread<int>("BackupThread"),
        mDataBase(database),
        mDestinationPath(destinationPath),
        mHourPeriod(hourPeriod)
{
    const int unusedItem = 0;
    cor::Time next; // now
    next += mHourPeriod * cor::Time::OneHour();
    next = next.TopOfMinute(cor::Time::eForwards, 0); // top of next minute
    PutItem(next, unusedItem);
}

SimpleBackupThread::~SimpleBackupThread()
{
    Stop();
}

void
SimpleBackupThread::HandleItem(const cor::Time& scheduled, const int& itemIgnored)
{
    // XXX hard-coded timezone
    const std::string timezone = "America/Denver";
    printf("\nSyncing database at %s %s local time\n", cor::LocalTime(timezone).Print().c_str(), timezone.c_str());

    printf("Remote (destination) is %s\n", mDestinationPath.c_str());

    std::shared_ptr<Database> remote;
    cor::Url remoteUrl;
    DatabaseImp* remoteFile = NULL;
    try
    {
        remoteUrl.Set(mDestinationPath);
        if ((remoteUrl.GetProtocol() == cor::Url::eHttp) ||
            (remoteUrl.GetProtocol() == cor::Url::eHttps))
        {
            remote.reset(new nabu::client::Database(remoteUrl.GetHost()));
        }
        else if (remoteUrl.GetProtocol() == cor::Url::eFile)
        {
            remoteFile = new DatabaseImp("/" + remoteUrl.GetResource());
            remote.reset(remoteFile);
        }
        else
        {
            printf("Protocol '%s' not recognized for backup destination\n",
                   remoteUrl.GetProtocolLiteral().c_str());
            return;
        }
    }
    catch (const cor::Exception& err)
    {
        printf("Error parsing destination URL: %s\n", err.what());
        remoteFile = new DatabaseImp(mDestinationPath);
        remote.reset(remoteFile);
    }

    try {
        if (remoteFile && !remoteFile->Exists())
        {
            remote->Clone(mDataBase.GetFingerprint().string(), mDataBase.GetPeriods());
        }
        if (remoteFile) remoteFile->Initialize();
    } catch (const cor::Exception& err)
    {
        printf("Needed to create a brand-new destination database and could not: %s\n", err.what());
        return;
    }


    try
    {
        if (remoteFile)
        {
            printf("Cleaning orphans on remote...\n");
            remoteFile->CleanOrphans();
            AsyncStatus localStats, remoteStats;
            while (true)
            {
                size_t proc;
                double pr = remoteFile->PercentDoneAsynchronous(remoteStats);
                fprintf(stderr, "\rremote: %02d%% of %ld",
                        (int)(pr), remoteStats.Total());

                if (remoteStats.Done())
                    break;

                usleep(250000);
            }
            printf("\nremote: %ld processed, %ld orphans removed\n",
                   remoteStats.Processed(), remoteStats.Count());
        }

        printf("Syncing...\n");
        nabu::CopyStats n = mDataBase.Sync(*remote);

        // empty the garbage of remote, so that no orphans are left
        if (remoteFile)
        {
            remoteFile->GetGarbage().Start();
            remoteFile->GetGarbage().WaitUntilEmpty();
            remoteFile->GetGarbage().Stop();
        }

        printf("Sync complete: %ld files moved, %ld nodes compared\n",
               n.mCopied,
               n.mCompared);
    } catch (const cor::Exception& err)
    {
        printf("!!!! Error syncing databases: %s->%s: %s\n",
               mDataBase.GetLiteral().c_str(),
               mDestinationPath.c_str(),
               err.what());
    }


    const int unusedItem = 0;
    cor::Time next;
    next += mHourPeriod * cor::Time::OneHour().Seconds();
    next = next.TopOfMinute(cor::Time::eForwards, 0); // next top of minute
    cor::LocalTime lt(timezone, next);
    printf("Next backup at %s %s local time\n", lt.Print().c_str(), timezone.c_str());
    PutItemReentrant(next, unusedItem);
}

} // end namespace
