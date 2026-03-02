//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/url.h>
#include <mcor/strutil.h>
#include <nabu_client/database.h>

#include "options.h"
#include "simulation.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

SimulationThread::SimulationThread(nabu::DatabaseImp& database,
        const std::string& sourceUrl,
        const std::string& sourceInstance,
        const cor::Time& start) :
    cor::Thread("SimulationThread"),
    mDataBase(database),
    mSourceUrl(sourceUrl),
    mSourceInstance(sourceInstance),
    mStart(start)
{
    DEBUG_LOCAL("Simulation Thread url = '%s', start = %s\n", sourceUrl.c_str(), mStart.Print().c_str());
}

SimulationThread::~SimulationThread()
{
    Stop();
}

void
SimulationThread::ThreadFunction()
{
    try 
    {
        nabu::client::Database sourceDatabase(mSourceUrl, mSourceInstance);
        
        std::vector<nabu::MetaKey> columns;
        columns = Options().SimulationColumns();

        // if no columns are specified in options, use all columns in source
        // database
        if (columns.empty())
        {
            nabu::BranchPtr b = sourceDatabase.GetBranch(nabu::MetaKey::MainBranch());
            b->GetColumns(nabu::MetaKey::EmptyAddress(), columns);
        }

        // this is the integer second time difference between the data as it
        // will appear in the destination from where it is pulled in the
        // source
        cor::Time delta = cor::Time().Seconds() - mStart.Seconds();
        DEBUG_LOCAL("Simulation time difference is %d seconds\n", delta.Seconds());

        // amount of time (roughly) to grab per loop
        const int chunk = Options().SimulationChunk(); // seconds

        // the last moment written to the destination
        cor::Time now;
        cor::Time lastDestination = now - Options().SimulationBackfillSeconds();

        while (!StopNow())
        {
            try
            {
                cor::TimeRange sourceTimeRange(lastDestination - delta, now - delta);
                cor::TimeRange destinationTimeRange(lastDestination, now);
                DEBUG_LOCAL("reading %s\n", sourceTimeRange.Print().c_str());
                //std::vector<nabu::RecordVector> records(columns.size());
                nabu::RecordMap records;
                for (size_t i = 0; i < columns.size(); i++)
                    records[columns[i]];

                // read from source
                {
                    nabu::AccessPtr sourceAccess = sourceDatabase.CreateAccess(
                            nabu::MetaKey::MainBranch(),
                            sourceTimeRange,
                            nabu::eRead);
                    sourceAccess->Open();

                    nabu::IOOperation::Read(sourceAccess, nabu::eIterateForwards, records, sourceTimeRange);
                }
                //printf("Read:\n");
                //records.PrintSummary();

                // update times in data; any TRR with the end in the future is
                // removed, to preserve causality
                size_t total = 0;
                nabu::RecordMap::iterator i = records.begin();
                for (; i != records.end(); i++)
                {
                    nabu::RecordVector& rv = i->second;
                    for (size_t j = 0; j < rv.size(); j++)
                    {
                        rv[j].mTimeRange += delta;
                        if (rv[j].mTimeRange.Final() > now)
                        {
                            /*printf("*** dropping non-causal TRR %s at %s (%d seconds early)\n",
                                   i->first.GetLiteral().c_str(),
                                   rv[j].mTimeRange.Final().Print().c_str(),
                                   (rv[j].mTimeRange.Final() - now).Seconds());*/
                            rv.erase(rv.begin() + j);
                        }
                        else
                        {
                            //printf("Record %s %ld time updated\n", i->first.GetLiteral().c_str(), j);
                            total++;
                        }
                    }
                }
                //printf("%ld total records updated: extents = %s\n", total, records.Extents().Print().c_str());

                // write to destination
                cor::TimeRange writeTr = records.Extents().SuperUnion(destinationTimeRange);

                if (writeTr.Valid())
                {
                    //printf("Writing to destination %s\n", writeTr.Print().c_str());
                    //records.PrintSummary();
                    {
                        nabu::AccessPtr destAccess = mDataBase.CreateAccess(
                                nabu::MetaKey::MainBranch(),
                                writeTr,
                                nabu::eWrite);
                        destAccess->Open();

                        nabu::IOOperationPtr destIop = destAccess.CreateIOOperation(destinationTimeRange, nabu::eIterateForwards);
                        destIop->Write(records);
                    }
                }

                lastDestination = now;
                //printf("Sleeping %d seconds\n", chunk);
                sleep(chunk);
                now = cor::Time();

            } catch (const cor::Exception& err)
            {
                printf("Error in simulation thread: %s\n", err.what());
                sleep(1);
            }
        }
        
    } catch (const cor::Exception& err)
    {
        printf("Error in simulation thread (exiting): %s\n", err.what());
    }
}
