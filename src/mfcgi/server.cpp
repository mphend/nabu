//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>
#include <sstream>
#include <stdio.h>

#include <mcor/strutil.h>
#include <mcor/mexception.h>

#include "exception.h"
#include "request.h"
#include "server.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace fcgi {

Server::Server(unsigned int port) :
    mPort(port),
    mSocketFd(0),
    mStopNow(false)
{
}

Server::~Server()
{
}

void
Server::Start()
{
    DEBUG_LOCAL("Starting FCGI connection on port %d\n", mPort);
    if (mSocketFd == 0)
    {
        int rc = FCGX_Init();
        if (rc == -1)
            throw cor::Exception("Could not initialize FCGX");

        const int backlog = 10;
        std::ostringstream oss;
        oss << ":" << mPort;

        rc = FCGX_OpenSocket(oss.str().c_str(), backlog);
        if (rc == -1)
            throw cor::ErrException("Could not initialize FCGI socket at port %d", mPort);
        mSocketFd = rc;
    }

    for (size_t i = 0; i < 32; i++)
    {
        mWorkerThreads.push_back(std::unique_ptr<WorkerThread>(new WorkerThread(*this)));
        mWorkerThreads.back()->Start();
    }
    DEBUG_LOCAL("FCGI server started with 32 worker threads%s\n", "");
}

void
Server::Stop()
{
    for (size_t i = 0; i < mWorkerThreads.size(); i++)
    {
        mWorkerThreads[i]->AskStop();
    }
    for (size_t i = 0; i < mWorkerThreads.size(); i++)
    {
        mWorkerThreads[i]->Stop();
    }
}

void
ParseParametersFromURL(const std::string& url, ParameterMap& parameters)
{
    std::string ps = url;
    ps.erase(0, ps.find_first_of('?'));
    if (!ps.empty())
    {
        ps.erase(0, 1); // remove the '?'
        std::vector<std::string> pv;
        cor::Split(ps, "&", pv);
        // pv is vector of key=value
        for (size_t i = 0; i < pv.size(); i++)
        {
            std::vector<std::string> kvv;
            cor::Split(pv[i], "=", kvv);

            if (kvv.size() != 2)
                throw Exception(400, "Bad Request: Ill-formed parameters");

            parameters[kvv[0]] = kvv[1];
        }
    }
}

Server::WorkerThread::WorkerThread(Server& server) :
    mServer(server),
    cor::Thread("FCGIServerWorker")
{}

void
Server::WorkerThread::ThreadFunction()
{
    // sanity
    if (mServer.mSocketFd == 0)
    {
        throw cor::ErrException("FCGI::Server() -- socket FD is zero");
    }

    DEBUG_LOCAL("FCGI Server worker thread 0x%lx starting\n", cor::Thread::Self());
    while (!StopNow())
    {
        try
        {
            FCGX_Request req;
            int flags = FCGI_FAIL_ACCEPT_ON_INTR;

            if (FCGX_InitRequest(&req, mServer.mSocketFd, flags) != 0)
            {
                printf("FCGX_InitRequest failed");
                continue;
            }

            if (FCGX_Accept_r(&req) != 0)
            {
                printf("FCGX_Accept_r failed");
                continue;
            }

            // Note: this object must be Commit()ed else there are
            // malloc issues

            Request request(&req);

            // this try/catch attempts to turn exceptions into error responses
            // to the remote caller. If this response raises an exception, the
            // outer try/catch in invoked.
            try {

                const Environment& e = request.GetEnvironment();
                //printf("Environment:\n%s\n", e.Print().c_str());
                std::string match = e.ReadString("SCRIPT_NAME");
                // remove leading/trailing slashes and spaces
                match = cor::Trim(match, " /");
                std::string method = e.ReadString("REQUEST_METHOD");

                ParameterMap parameters;
                std::string ps = e.ReadString("REQUEST_URI");
                ParseParametersFromURL(ps, parameters);

                HandlerMap::iterator hmci = mServer.mHandlers.begin();
                DEBUG_LOCAL("Looking up handler for %s\n", match.c_str());
                for (; hmci != mServer.mHandlers.end(); hmci++)
                {
                    KeyedFieldMap keyed;
                    if (hmci->first.Equals(match, method, keyed))
                    {
                        std::string debug = hmci->second->DebugString(request, keyed, parameters).c_str();
                        DEBUG_LOCAL("Running handler %s\n", debug.c_str());
                        mServer.ExecuteHandler(hmci->second, request, keyed, parameters);
                        DEBUG_LOCAL("handler %s completed\n", debug.c_str());
                        break;
                    }
                }

                if (hmci == mServer.mHandlers.end())
                {
                    printf("Failed to find handler for %s:%s\n", match.c_str(), method.c_str());
                    throw fcgi::Exception(404, "Resource '%s' not found", match.c_str());
                }
                request.AssertWriterStream();
            } catch (const fcgi::Exception& err)
            {
                DEBUG_LOCAL("Handling fcgi exception '%s'\n", err.what());

                fcgi::Header responseHeader;
                request.Reply(err.Code(), responseHeader, (std::string)err.what());
            }
            catch (const cor::Exception& err)
            {
                DEBUG_LOCAL("Handling exception '%s'\n", err.what());

                fcgi::Header responseHeader;
                request.Reply(500, responseHeader, (std::string)err.what());
            }

            request.Commit();

        }
        catch (const cor::Exception& err)
        {
            printf("%s\n", err.what());
        }
        catch (const std::exception& err)
        {
            printf("%s\n", err.what());
        }
        catch (...)
        {
            printf("%s eating unknown error\n", GetName().c_str());
        }
    }

    DEBUG_LOCAL("FCGI Server worker thread ending%s\n", "");
    // no more new requests
    FCGX_ShutdownPending();
}

void
Server::RegisterHandler(Handler& handler, const std::string& url, std::string methods)
{
    UrlMatcher m(url, methods);
    mHandlers[m] = &handler;

    DEBUG_LOCAL("Attaching %s endpoint at %s\n",
               handler.Description().c_str(),
               m.GetLiteral().c_str());
}

std::string
Server::PrintEndpoints() const
{
    std::string r;
    HandlerMap::const_iterator i = mHandlers.begin();
    for (; i != mHandlers.end(); i++)
    {
        char buffer[400];
        snprintf(buffer, 400, "%-20s   %s\n",
               i->second->Description().c_str(),
               i->first.GetLiteral().c_str());
        r += buffer;
    }
    return r;
}

}
