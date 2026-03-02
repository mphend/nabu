//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef PROC_NABU_SERVER_OPTIONS_INCL
#define PROC_NABU_SERVER_OPTIONS_INCL

#include <string>
#include <map>
#include <set>
#include <vector>

#include <mcor/mexception.h>
#include <nabu/metakey.h>
#include <mcor/json_config.h>


/** Class ProgOptions
 *      Program options that never change while the process is
 *      running; stuff that could be command-line arguments, or hard-coded
 *      constants, but are instead slurped from a JSON file.
 *
 *      If the option is not in the JSON file, whatever the default constructor
 *      of that type creates will be returned.  There is no checking.
 *
 */


class ProgOptions : public cor::JsonConfig
{
public:
    ProgOptions() : cor::JsonConfig() {}

    // get directory of database
    std::string NabuDirectory();

    // web port number
    int WebPortNumber() const;

    // URL prefix
    // "nabu/v1" or similar
    std::string Prefix() const;

    // cache size limit (in entries)
    int CacheSizeLimit() const;

    // number of seconds of inactivity after which an IO Operation is discarded
    int IOAccessTimeoutSeconds() const;

    // number of seconds that a single Select call is allowed to wait
    // (the total wait time is unlimited, but it is broken up into chunks
    //  this long each, if necessary)
    int SelectBlockPeriodSeconds() const;

    // log level
    std::string LogLevel() const;

    // log echo to stdout
    bool LogEcho() const;

    // simulation columns
    std::vector<nabu::MetaKey> SimulationColumns() const;

    // simulation update period, in seconds
    int SimulationChunk() const;

    // simulation data backfill, in seconds
    int SimulationBackfillSeconds() const;
};


// ============================================================================
// public functions
//

// return the global options instance
ProgOptions& Options();

#endif
