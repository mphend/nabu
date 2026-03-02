//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <mcor/url.h>
#include <mfcgi/exception.h>

#include <nabu_client/database.h>

#include "uri_sync.h"


namespace nabu {


void
SyncHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::string destination;

    destination = parameters.GetString("destination");
    bool init = parameters.GetBool("initialize", false);

    DatabaseImpPtr database = GetDatabase(keyedFields);
    DatabasePtr remote;
    cor::Url url(destination);
    if (url.GetProtocol() == cor::Url::eFile)
    {
        remote.reset(new DatabaseImp(url.GetResource()));
    }
    else if ((url.GetProtocol() == cor::Url::eHttp) ||
             (url.GetProtocol() == cor::Url::eHttps))
    {
        remote.reset(new client::Database(destination));
    }
    else
        throw fcgi::Exception(400, "Unsupported remote protocol '%s'",
                              url.GetProtocolLiteral().c_str());

    // XXX not sure if this should be supported; maybe only for
    // 'file' (local) type operation?
    /*
    if (init)
        remote->CreateNew(database->GetTimeScheme());
    */
    if (init)
        throw fcgi::Exception(400, "init=true is not currently supported");

    nabu::CopyStats n = database->Sync(*remote);

    Json::Value v;
    v["copied"] = (int)n.mCopied;
    v["compared"] = (int)n.mCompared;

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


} // end namespace
