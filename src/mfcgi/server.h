//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_FCGI_SERVER_H
#define GICM_FCGI_SERVER_H

#include <map>
#include <memory>
#include <string>

#include <fcgiapp.h>

#include <mcor/mfile.h>
#include <mcor/mthread.h>

#include "handler.h"
#include "url_matcher.h"

namespace fcgi {


/** class Server : Wraps the library and listening socket
 *
 *  This is NOT a web server. It is not threadsafe; register everything
 *  before starting it.
 *
 */
class Server
{
public:
    // 'port' is the port number the web server will talk to this with
    Server(unsigned int port);
    virtual ~Server();

    // these calls handle creation of the socket and protocol with the
    // proxy server, and the threads used to service requests. Make sure
    // Stop is called before destroying this object.
    void Start();
    void Stop();

    bool IsStopped() const { return mStopNow; }
    void StopNow() { mStopNow = true; }

    // register callbacks based on URL, etc., similar to Flask
    //    if 'methods' is empty, all methods are a match; otherwise, slash delimited
    //       set of methods is used, e.g. "GET/POST/PUT"
    // see UrlMatcher for interpretation of urlMatch string
    void RegisterHandler(Handler& handler, const std::string& urlMatch, std::string methods = "");

    // override of this allows subclasses to specialize how they handle exceptions
    virtual void ExecuteHandler(Handler* handler, Request& request, KeyedFieldMap& keyed, ParameterMap& parameters)
    {
        handler->Process(request, keyed, parameters);
    }

    // generate the endpoint attachments
    std::string PrintEndpoints() const;

private:
    unsigned int mPort;
    int mSocketFd;

    bool mStopNow;

    typedef std::map<UrlMatcher, Handler*> HandlerMap;
    HandlerMap mHandlers;

    class WorkerThread : public cor::Thread
    {
    public:
        WorkerThread(Server& server);
        void ThreadFunction();
    private:
        Server& mServer;
    };

    std::vector<std::unique_ptr<WorkerThread>> mWorkerThreads;

    friend class WorkerThread;
};

}

#endif
