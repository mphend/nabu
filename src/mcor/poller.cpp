//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <unistd.h>

#include "poller.h"

namespace cor {

PollingThread::PollingThread(const std::string& name, int pollingPeriodSeconds) :
    cor::Thread(name),
    mPollingPeriodSeconds(pollingPeriodSeconds),
    mMutex(name + "_Mutex")
{
}

void
PollingThread::AddPollable(Pollable* p)
{
    cor::ObjectLocker ol(mMutex);
    mImp.insert(p);
}

void
PollingThread::RemovePollable(Pollable* p)
{
    cor::ObjectLocker ol(mMutex);
    mImp.erase(p);
}

void
PollingThread::ThreadFunction()
{
    while(!StopNow())
    {
        {
            cor::ObjectLocker ol(mMutex);
            Imp::iterator i = mImp.begin();
            for (; i != mImp.end(); i++)
            {
                try
                {
                    (*i)->Poll();
                } catch (const cor::Exception& err)
                {
                    std::cout << "PollingThread backstop: " << err.what() << std::endl;
                }
            }
        }
        sleep(mPollingPeriodSeconds);
    }
    
}

}
