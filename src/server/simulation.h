//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SIMULATION_H
#define GICM_NABU_SIMULATION_H


#include <nabu/database_imp.h>


/* SimulationThread
 *
 */
class SimulationThread : public cor::Thread
{
public:
    SimulationThread(nabu::DatabaseImp& database,
                     const std::string& sourceUrl,
                     const std::string& sourceInstance,
                     const cor::Time& start);
    virtual ~SimulationThread();

protected:
    nabu::DatabaseImp& mDataBase;
    std::string mSourceUrl;
    std::string mSourceInstance;
    cor::Time mStart;

    void ThreadFunction();
};

#endif
