//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "nabu_server.h"

#include <nabu/exception.h>

// handlers
#include "uri_access.h"
#include "uri_append.h"
#include "uri_branch.h"
#include "uri_columns.h"
#include "uri_database.h"
#include "uri_extents.h"
#include "uri_iop.h"


#include "options.h"

namespace nabu {

Server::Server(DatabaseMap& databases, const std::string& prefix, unsigned int port) :
    fcgi::Server(port),
    mDatabases(databases),
    mPrefix(prefix),
    mIOPCache("IOP", Options().IOAccessTimeoutSeconds()),
    // add a few seconds to try to let IO accesses time out first;
    // if not, this is handled by the server, but it is less expected
    mAccessCache("Access", Options().IOAccessTimeoutSeconds() + 5)
{
    CreateHandlers();
}


Server::~Server()
{
}

void
Server::Start()
{
    fcgi::Server::Start();
    mIOPCache.Start();
    mAccessCache.Start();
}

void
Server::Stop()
{
    fcgi::Server::Stop();
    mAccessCache.Stop();
    mIOPCache.Stop();
}

void
Server::ExecuteHandler(fcgi::Handler* handler,
                    fcgi::Request& request,
                    fcgi::KeyedFieldMap& keyed,
                    fcgi::ParameterMap& parameters)
{
    try {
        handler->Process(request, keyed, parameters);
    } catch (nabu::FatalException& err)
    {
        // if the database URL is empty, then it is the cache or another
        // non-database-specific fatal exception, and the entire server
        // should stop
        if (!err.mDatabaseUrl.Valid())
        {
            printf("\n**** Fatal exception");
            fcgi::Server::StopNow();
            throw;
        }

        printf("\n**** Fatal exception in database at %s:\n", err.mDatabaseUrl.GetLiteral().c_str());
        // for fatal exceptions, take the database offline
        std::string key;
        if (mDatabases.FindByUrl(err.mDatabaseUrl, key))
        {
            printf("taking database %s offline\n", key.c_str());
            mDatabases.GoOffline(key);
        }
        printf("%s\n", err.what());
    }
}

void
Server::CreateHandlers()
{
    printf("Creating handlers at prefix %s\n", mPrefix.c_str());

    RegisterHandler(*(new ListDatabasesHandler(mDatabases)), mPrefix +
        "databases", "GET");
    RegisterHandler(*(new FingerprintHandler(mDatabases)), mPrefix +
        "database/<database>/fingerprint", "POST");
    RegisterHandler(*(new TimePeriodsHandler(mDatabases)), mPrefix +
        "database/<database>/time_periods", "POST");
    RegisterHandler(*(new VersionInUseHandler(mDatabases)), mPrefix +
        "database/<database>/versioninuse", "POST");
    RegisterHandler(*(new CloneHandler(Options().NabuDirectory(), mDatabases)), mPrefix +
        "database/clone", "POST");
    RegisterHandler(*(new ReadFileHandler(mDatabases)), mPrefix +
        "database/<database>/readfile", "POST");
    RegisterHandler(*(new WriteFileHandler(mDatabases)), mPrefix +
        "database/<database>/writefile", "POST");

    RegisterHandler(*(new GetBranchHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>", "GET");
    RegisterHandler(*(new CreateBranchHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>", "POST");
    RegisterHandler(*(new MoveHeadHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/movehead", "POST");
    RegisterHandler(*(new CreateTagHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/tag", "POST");
    RegisterHandler(*(new DeleteTagHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/tag/<tag_name>", "DELETE");
    RegisterHandler(*(new GetTagHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/tag/<tag_name>", "GET");
    RegisterHandler(*(new GetHeadOfTagHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/tag/<tag_name>/head", "POST");
    RegisterHandler(*(new ListTagsHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/tags", "GET");

    RegisterHandler(*(new TrimTagsHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/trimtags", "POST");
    RegisterHandler(*(new ColumnsHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/columns", "GET");
    RegisterHandler(*(new DeleteColumnBranchHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/column/<column>", "DELETE");
    RegisterHandler(*(new HasColumnHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/column/<column>", "GET");
    RegisterHandler(*(new ExtentsHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/extents/<column>", "GET");
    RegisterHandler(*(new AppendHandler(mDatabases)), mPrefix +
        "database/<database>/branch/<branch_path>/append/<column>", "POST");
    RegisterHandler(*(new NewAccessHandler(mDatabases, mAccessCache, mIOPCache)), mPrefix +
        "database/<database>/branch/<branch_path>/access", "POST");

    RegisterHandler(*(new TimeoutHandler(Options().IOAccessTimeoutSeconds())), mPrefix +
        "policy/timeout", "GET");

    RegisterHandler(*(new QueryAccessHandler(mDatabases, mAccessCache, 30)), mPrefix +
        "access/<access_id>", "POST");
    RegisterHandler(*(new CloseAccessHandler(mDatabases, mAccessCache)), mPrefix +
        "access/<access_id>", "DELETE");
    RegisterHandler(*(new SelectAccessHandler(mDatabases, mAccessCache, 30)), mPrefix +
        "access/<access_id>/select", "POST");
    RegisterHandler(*(new ExtentsAccessHandler(mDatabases, mAccessCache)), mPrefix +
        "access/<access_id>/extents", "POST");
    RegisterHandler(*(new NewIopHandler(mDatabases, mIOPCache, mAccessCache)), mPrefix +
        "access/<access_id>/iop", "POST");
    RegisterHandler(*(new TickHandler(mDatabases, mAccessCache)), mPrefix +
        "access/<access_id>/tick", "PUT");

    RegisterHandler(*(new CloseIopHandler(mDatabases, mIOPCache)), mPrefix +
        "iop/<operation_id>", "DELETE");
    RegisterHandler(*(new CommitIopHandler(mDatabases, mIOPCache)), mPrefix +
        "iop/<operation_id>/commit", "POST");
    RegisterHandler(*(new ReadIopHandler(mDatabases, mIOPCache)), mPrefix +
        "iop/<operation_id>/read/<column>", "POST");
    RegisterHandler(*(new ReadSeekIopHandler(mDatabases, mIOPCache)), mPrefix +
        "iop/<operation_id>/read_seek/<column>", "POST");
    RegisterHandler(*(new WriteIopHandler(mDatabases, mIOPCache)), mPrefix +
        "iop/<operation_id>/write/<column>", "POST");

    // Dump the handlers
    printf("Handlers:\n%s\n", PrintEndpoints().c_str());
}

}
