//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mcor/url.h>
#include <mcor/arg_map.h>
#include <mcor/mexception.h>
#include <mcor/thread_analysis.h>
#include <nabu/context.h>
#include <nabu/database.h>
#include <nabu/exception.h>

#include <nabu_client/database.h>

#include "nabu_server.h"
#include "options.h"
#include "simulation.h"


void Usage()
{
    std::cout << "Commands:" << std::endl;
    std::cout << "--init <nabu directory>" << std::endl;
    std::cout << "    create a skeleton database and exit" << std::endl;
    std::cout << "--sync <url>" << std::endl;
    std::cout << "    make distant server the same as this one" << std::endl;
    std::cout << "--config <path=/etc/nabu/local/nabu_overrides.conf>" << std::endl;
    std::cout << "    use the given overrides config file" << std::endl;
    std::cout << "--simulation <dest database> <source url> %Y-%m-%d %H:%M:%S" << std::endl;
    std::cout << "    simulate an active database by reading from another at given time" << std::endl;
}

void sig_int_handler(int signum)
{
    printf("\nNabu Server: stopping\nThreads:");
    cor::ThreadAnalysis::Get().Dump();

    printf("Database context:\n");
    nabu::DumpContext();

    // Be aware that calling exit() destroys static object, but does not unwind
    // the stack. Threads will continue to run during this.
    exit(0);
}

void sig_float_exception_handler(int signum)
{
    printf("Nabu Server: Floating point exception signal caught\nDatabase context:\n");
    nabu::DumpContext();
    exit(0);
}

int main(int argc, const char * argv[])
{
    signal(SIGINT, sig_int_handler);
    signal(SIGFPE, sig_float_exception_handler);

    try {

        cor::ArgMap argMap;
        argMap.Parse(argv, argc);

        // load the config for this program
        std::string overridesConfigFile = "/etc/nabu/local/nabu_overrides.conf";
        if (argMap.Find("config"))
        {
            overridesConfigFile = argMap.Next();
        }

        Options().Merge(std::string("/etc/nabu/nabu.conf"));
        if (!Options().MergeIfExists(std::string("/etc/nabu/local/nabu_installation.conf")))
            printf("Warning: no installation file found (/etc/nabu/local/nabu_installation.conf)\n");
        Options().MergeIfExists(overridesConfigFile);

        bool init = false;
        if (argMap.Find("help"))
        {
            Usage();
            return 0;
        }

        //  just create a skeleton database and exit
        if (argMap.Find("init"))
        {
            std::vector<unsigned int> periods;
            periods.push_back(86400);  //  layer 0:  address/X_Y_Z.dat files
            periods.push_back(100);  //  layer 1:  address/X_Y.*.met files
            periods.push_back(10);  //  layer 2:  address/X.*.met files

            std::string root = argMap.Next(); // destination directory name
            nabu::DatabaseImp brandNewDataBase(root);
            brandNewDataBase.CreateNew(periods);

            printf("Don't forget to change /etc/nabu.conf to refer to this directory if you want\n");
            printf("the nabu server to start using it.\n");
            return 0;
        }

        // simulation
        bool simulation = argMap.Find("simulation");
        std::string simulationDestinationDatabase;
        std::string simUrl;
        std::string simInstance;
        cor::Time simTime;
        if (simulation)
        {
            simulationDestinationDatabase = argMap.Next();
            simUrl = argMap.Next();
            simInstance = argMap.Next();

            std::string dateclump = argMap.Next();
            std::string hourclump = argMap.Next();
            simTime = cor::Time(dateclump + " " + hourclump, "%Y-%m-%d %H:%M:%S");
        }

        nabu::DatabaseMap databases(Options().NabuDirectory(), Options().SelectBlockPeriodSeconds());
        try {
            databases.Load();
        } catch (const cor::Exception& err)
        {
            printf("Warning: %s\n", err.what());
            printf("Adding databases automatically...\n");
            try {
                databases.Deduce();
            }
            catch (const cor::Exception& err)
            {
                printf("Could not deduce databases: %s\n", err.what());
                printf("Exiting\n");
                exit(0);
            }
        }
        printf("Serving the following databases:\n");
        printf("%s\n", databases.Print().c_str());

        // set up each database
        {
            cor::ObjectLocker ol;
            nabu::DatabaseMap::Iterator i = databases.Begin(ol);

            for (; i != databases.End(); i++)
            {
                nabu::DatabaseMap::Entry dbe = i->second;
                try
                {
                    dbe.mDatabase->Initialize();
                    dbe.mDatabase->SetCacheLimit(Options().CacheSizeLimit());

                    // clean orphans
                    printf("Cleaning orphans...\n");
                    nabu::AsyncStatus status = dbe.mDatabase->CleanOrphans(std::cout);
                    if (status.Count() != 0)
                        printf("\n%ld orphans found in %ld files\n", status.Count(), status.Processed());

                    // check for missing files
                    printf("Checking for missing files...\n");
                    status = dbe.mDatabase->CountMissing(std::cout);
                    if (status.Count() != 0)
                    {
                        printf("\n%ld missing found in %ld files\n", status.Count(), status.Processed());
                        throw cor::Exception("Database '%s' is corrupted, %ld files missing",
                                             i->first.c_str(),
                                             status.Count());
                    }

                    // flush journals
                    printf("Flushing journals\n");
                    dbe.mDatabase->FlushJournals();

                    // start journal flushing thread
                    printf("Starting journaling\n");
                    dbe.mDatabase->GetJournalThread().Start();

                    // start garbage collection
                    printf("Starting garbage collection\n");
                    dbe.mDatabase->GetGarbage().Start();
                }
                catch (const cor::Exception& err)
                {
                    printf("Could not initialize database '%s' : %s\n",
                           i->first.c_str(), err.what());
                    databases.GoOffline(i->first);
                    printf("Database '%s' is OFFLINE\n", i->first.c_str());
                }
            }
        } // end lock scope on DatabaseMap

        // start a server
        nabu::Server server(databases, Options().Prefix(), Options().WebPortNumber());
        server.Start();

        if (simulation)
        {
            nabu::DatabaseImpPtr dest;
            if (databases.Get(simulationDestinationDatabase, dest) == nabu::eOffline)
                throw cor::Exception("Simulation destination database '%s' is offline",
                                     simulationDestinationDatabase.c_str());
            // reads from database at simUrl:simPort from time simStart and fills
            // data in destination database
            SimulationThread sim(*dest, simUrl, simInstance, simTime);
            sim.Start();
            while (true)
                sleep(1);
        }


        while (!server.IsStopped())
        {
            sleep(1);
        }

        server.Stop();


    } catch (const nabu::FatalException& err) {
        std::cout << "Fatal Error: " << err.what() << std::endl;
        abort();
    }
    catch (const cor::Exception& err) {
        std::cout << "Error: " << err.what() << std::endl;
        abort();
    } catch (const std::exception& err) {
        std::cout << "std Error: " << err.what() << std::endl;
        abort();
    }

    return 0;
}

