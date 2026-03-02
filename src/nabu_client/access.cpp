//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <interweb/request.h>
#include <json/reader.h>
#include <mcor/json_time.h>
#include <json/writer.h>
#include <json/value.h>

#include <mcor/binary.h>
#include <mcor/strutil.h>

#include <nabu/io_op.h>
#include <nabu/tag_imp.h>

#include "access.h"
#include "branch.h"
#include "iop.h"
#include "tag.h"

#define DEBUG 1
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu {
namespace client {

/** Class TickThread
 *
 */
class TickThread : public cor::Thread
{
public:
    TickThread(nabu::Access* access) : cor::Thread("Tick"), mAccess(access)
    {}

    nabu::Access* mAccess;

    void ThreadFunction()
    {
        while (!StopNow())
        {
            try {
                mAccess->Tick();
            } catch (const cor::Exception& err)
            {
                printf("tick error: %s\n", err.what());
            }
            Wait(10);
        }
    }
};

Access::Access(const cor::Url& url,
               nabu::BranchPtr branch,
               const muuid::uuid& handle,
               nabu::AccessType type,
               const cor::TimeRange& timeRange,
               const std::string& tagName) :
    nabu::Access(branch, handle, type, timeRange, tagName),
    mUrl(url),
    mBranch(branch),
    mOpen(false),
    mBorrowed(false),
    mTickThread(NULL)
{
    mTimeRange = timeRange;

    mTickThread = new TickThread(this);
    mTickThread->Start();
}

Access::~Access()
{
    //printf("~Access() %s %s\n", IsRead() ? "(read)" : "(write)", mTimeRange.Print().c_str());
    if (mTickThread)
    {
        mTickThread->Stop();
        delete mTickThread;
        mTickThread = NULL;
    }

    if (mBorrowed)
        return;

    // must eat exceptions that Close() could throw
    try {
        Close();
    } catch (...)
    {}
}

nabu::IOOperationPtr
Access::CreateIOOperation(const cor::TimeRange& timeRange, IterationDirection direction)
{
    DEBUG_LOCAL("Access(client)::CreateIOOperation %s%s\n",
                timeRange.Print().c_str(),
                direction == eIterateForwards ? "" : " (backwards)");

    // Create the new IO operation
    try {
        http::Request rq;
        http::ParameterMap p;
        if (timeRange.First().IsOrdinary())
            p.SetTime("start", timeRange.First());
        if (timeRange.Final().IsOrdinary())
            p.SetTime("end", timeRange.Final());
        p.SetBool("backwards_iteration", direction == eIterateBackwards);

        http::RequestPtr response = rq.Post(mUrl + "/access/" + GetHandle().string() + "/iop", p);

        response->AssertReturnCode(200);
        muuid::uuid handle;
        handle.set(response->GetJson().asString());

        IOOperationPtr r(new IOOperation(mUrl, this, timeRange, direction, handle));
        return r;

    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::CreateIOOperation() -- %s", err.what());
        throw err;
    }
}

bool
Access::Open(int secWait)
{
    DEBUG_LOCAL("Access(client)::Open%s\n", "");
    try
    {
        // SANITY
        if (mHandle.is_nil())
            throw cor::Exception("Access has not yet been created");

        // this keeps track of the total time waited, since the server may
        // choose to wait less in order to keep the socket happy
        int timeoutSecIn = secWait;
        if (secWait < 0)
            timeoutSecIn = -1; // all negative numbers are mapped to this
        int timeoutSec = timeoutSecIn;

        while (!mOpen)
        {

            http::Request rq;
            http::ParameterMap p;
            p.SetInteger("timeout", timeoutSec);
            if (!mTagName.empty())
                p.SetString("tag", mTagName);

            http::RequestPtr response = rq.Post(mUrl + "/access/" + mHandle.string(), p, 35);

            if (response->Code() == 200)
            {
                mOpen = true;
            }
            else if (response->Code() == 202)
            {
                // if this is not an infinite timeout, decrement the total time
                // waited
                if (timeoutSecIn >= 0)
                {
                    timeoutSec -= response->GetJson().asInt();

                    // DEFENSIVE
                    if (timeoutSec < 0) // went negative, should not happen
                    {
                        printf("Warning: finite timeout count went negative\n");
                        timeoutSec = 0;
                    }
                }
            }
            else
            {
                // any other code, throw; it can't be 200
                response->AssertReturnCode(200);
            }

            if (timeoutSec == 0)
                break; // timed out (outer loop)
        }
        return mOpen;
    }
    catch (cor::Exception& err)
    {

        err.SetWhat("Access::Open() -- %s", err.what());
        throw err;
    }
}

bool
Access::IsOpen() const
{
    return mOpen;
}

SelectResult
Access::Select(const std::set<nabu::MetaKey>& columns,
               nabu::WrittenColumnsMap& wcm,
               int timeoutSecIn)
{
    DEBUG_LOCAL("Access(client)::Select() -- timeout = %d\n", timeoutSecIn);
    if (!IsOpen())
        throw cor::Exception("Cannot Select() when Access is not yet Open");

    try {

        while (true)
        {
            http::Request rq;
            http::ParameterMap p;

            // encode column set into JSON
            http::JsonPtr v(new Json::Value);
            std::set<nabu::MetaKey>::const_iterator j = columns.begin();
            for (int i = 0; i < columns.size(); i++, j++)
            {
                (*v)[i] = j->GetLiteral();
            }
            rq.SetJson(v);

            // be willing to wait whatever is our timeout, plus 60 seconds; the far
            // end may not block that long
            int callTimeoutSeconds = 60;
            {
                if (timeoutSecIn > 0)
                    callTimeoutSeconds += timeoutSecIn;
                p.SetInteger("timeout", timeoutSecIn);
            }

            http::RequestPtr response = rq.Post(mUrl + "/access/" + mHandle.string() + "/select",
                                                p,
                                                callTimeoutSeconds);

            if (response->Code() == 202) // wait again
            {
                continue; // wait again
            }
            else if (response->Code() == 408) // request timeout
            {
                return eTimeout;
            }
            else if (response->Code() == 410) // gone
            {
                return eAbort;
            }
            else
            {
                response->AssertReturnCode(200);
                // parse WCM
                wcm.Parse(response->GetJson());
                return eComplete;
            }

        } // end while
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::Select() -- %s", err.what());
        throw err;
    }
}

bool
Access::AbortSelect()
{
    DEBUG_LOCAL("Access(client)::AbortSelect%s\n", "");
    if (!IsOpen())
        throw cor::Exception("Cannot call AbortSelect() when Access is not yet Open");

    try {
        http::ParameterMap p;
        http::Request rq;

        http::RequestPtr response = rq.Post(mUrl + "/access/" + mHandle.string() + "/abortselect", p);
        response->AssertReturnCode(200);

        return response->GetJson().asBool();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::AbortSelect() -- %s", err.what());
        throw err;
    }
}

void
Access::Extents(const std::set<MetaKey>& columns, WrittenColumnsMap& wcm)
{
    DEBUG_LOCAL("Access(client)::Extents%s\n", "");
    if (!IsOpen())
        throw cor::Exception("Cannot call Extents() when Access is not yet Open");

    try {
        http::ParameterMap p;
        http::Request rq;

        // encode column set into JSON
        http::JsonPtr v(new Json::Value);
        std::set<nabu::MetaKey>::const_iterator j = columns.begin();
        for (int i = 0; i < columns.size(); i++, j++)
        {
            (*v)[i] = j->GetLiteral();
        }
        rq.SetJson(v);

        http::RequestPtr response = rq.Post(mUrl + "/access/" + mHandle.string() + "/extents", p);
        response->AssertReturnCode(200);

        wcm.Parse(response->GetJson());
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::Extents() -- %s", err.what());
        throw err;
    }
}
/*
std::shared_ptr<nabu::Tag>
Access::CreateTag(const std::string& newTagName, TagPolicy tagPolicy)
{
    DEBUG_LOCAL("Access(client)::CreateTag %s policy %s\n",
                newTagName.c_str(),
                TagPolicyToString(tagPolicy).c_str());
    try {
        http::ParameterMap p;
        p.SetString("name", newTagName);
        p.SetString("policy", TagPolicyToString(tagPolicy));
        http::Request rq;

        http::RequestPtr response = rq.Post(mUrl + "/access/" + mHandle.string() + "/tag", p);
        response->AssertReturnCode(200);

        Version version;
        version.ParseFromJson(response->GetJson());

        TagPtr r(new nabu::client::Tag(mUrl, *GetBranch(), newTagName, version));
        return r;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::CreateTag() -- %s", err.what());
        throw err;
    }
}

void
Access::DeleteTag()
{
    DEBUG_LOCAL("Access(client)::DeleteTag%s\n", "");
    try {
        http::ParameterMap p;
         http::Request rq;

        http::RequestPtr response = rq.Delete(mUrl +
                "/access/" + mHandle.string() +
                "/tag/" + mTagName, p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::DeleteTag() -- %s", err.what());
        throw err;
    }
}

void
Access::DuplicateTag(const std::string& newTagName)
{
    DEBUG_LOCAL("Access(client)::DuplicateTag%s\n", "");
    try {
        // I don't think this has been used or tested
        // MPH Feb 2025
        throw cor::Exception("Unimplemented function");

        http::ParameterMap p;
        p.SetString("name", newTagName);
        http::Request rq;

        http::RequestPtr response = rq.Post(mUrl + "/access/" + mHandle.string() + "/tag", p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::DuplicateTag() -- %s", err.what());
        throw err;
    }
}
*/
void
Access::Tick()
{
    try {
        http::ParameterMap p;
        http::Request rq;

        http::RequestPtr response = rq.Put(mUrl + "/access/" + mHandle.string() + "/tick", p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::Tick() -- %s", err.what());
        throw err;
    }
}

void
Access::DisableGarbageCollection()
{
    try {
        http::ParameterMap p;
        http::Request rq;

        p.SetBool("disable", true);

        http::RequestPtr response = rq.Put(mUrl + "/access/" + mHandle.string() + "/garbage", p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::DisableGarbageCollection() -- %s", err.what());
        throw err;
    }
}

void
Access::EnableGarbageCollection()
{
    try {
        http::ParameterMap p;
        http::Request rq;

        p.SetBool("disable", false);

        http::RequestPtr response = rq.Put(mUrl + "/access/" + mHandle.string() + "/garbage", p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::EnableGarbageCollection() -- %s", err.what());
        throw err;
    }
}

const muuid::uuid&
Access::GetHandle() const
{
    // Don't allow stringification of an invalid handle
    if (mHandle.is_nil())
    {
        throw cor::Exception("Access::GetHandle() -- access is not yet opened\n");
    }

    return mHandle;
}
void
Access::Render(Json::Value& v)
{
    v["url"] = mUrl.GetLiteral();
    v["uuid"] = mHandle.string();
    v["type"] = mType == eRead ? "read" : "write";
    Json::TimeRangeToJson(mTimeRange, v["timerange"]);
}

void
Access::Close()
{
    DEBUG_LOCAL("Access(client)::Close%s\n", "");
    // SANITY
    if (mHandle.is_nil())
        return;

    try {
        http::ParameterMap p;
        http::Request rq;

        http::RequestPtr response = rq.Delete(mUrl + "/access/" + mHandle.string(), p);
        response->AssertReturnCode(200);

        mHandle.string() = "";
        mOpen = false;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Access::Close() -- %s", err.what());
        throw err;
    }
}


} // end namespace
}
