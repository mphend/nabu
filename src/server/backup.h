//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_BACKUP_H
#define GICM_NABU_SERVER_BACKUP_H


#include <mcor/wall_clock_thread.h>
#include <nabu/database_imp.h>

namespace nabu {


/* BackupThread
 *
 *  This creates a set of up to 12 complete backups of the source database
 *  First of month ("StartOfMonth")
 *  First of week (Sunday) ("StartOfWeek1", etc.)
 *  Day of week (except Sunday) ("Monday", "Tuesday", etc.)
 *
 *  There is no memory of whether the sync has occurred, so on some days
 *  the sync will occur all day on multiple destinations. For instance,
 *  if the first day of the month is on a Tuesday, then the StartOfMonth
 *  database and the Tuesday database will both be destinations all day.
 *  This means that on Wednesday morning, Tuesday and StartOfMonth will
 *  be identical, even if changes occurred during that previous day.
 *
 */
class BackupThread : public cor::WallClockThread<int>
{
public:
    BackupThread(nabu::DatabaseImp& database, const std::string& destinationPath, double hourPeriod);
    virtual ~BackupThread();

protected:
    nabu::DatabaseImp& mDataBase;
    std::string mDestinationPath;
    double mHourPeriod;

    void HandleItem(const cor::Time& scheduled, const int& item);
};


/* SimpleBackupThread
*
*  This creates a single backup at a destination
*
*/
class SimpleBackupThread : public cor::WallClockThread<int>
{
public:
    SimpleBackupThread(nabu::DatabaseImp& database, const std::string& destinationPath, double hourPeriod);
    virtual ~SimpleBackupThread();

protected:
    nabu::DatabaseImp& mDataBase;
    std::string mDestinationPath;
    double mHourPeriod;

    void HandleItem(const cor::Time& scheduled, const int& item);
};

}

#endif
