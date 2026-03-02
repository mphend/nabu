//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <stdio.h>
#include <string.h>

#include <curl/curl.h>

#include <mcor/strutil.h>

#include "http_headers.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace http {

HeaderMap::HeaderMap() : mCode(0), mHeadersCurl(NULL)
{}

HeaderMap::~HeaderMap()
{
    if (mHeadersCurl != NULL)
        curl_slist_free_all(mHeadersCurl);
}

void
HeaderMap::Parse(const std::string& headers)
{
    clear();

    if (mHeadersCurl != NULL)
    {
        curl_slist_free_all(mHeadersCurl);
        mHeadersCurl = NULL;
    }

    std::vector<std::string> lines;
    cor::Split(headers, "\n", lines);

    // some lines are of form
    //    HTTP/1.1 <code> <message>
    // there can be multiple of these in the event of a server
    // redirect
    //
    // remainder are
    //    <key>: value (spaces allowed)

    for (size_t i = 0; i < lines.size(); i++)
    {
        std::string line = cor::Trim(lines[i]);

        if (line.empty())
            continue;
        std::string::size_type colon = line.find(':');

        // non-colon lines are "HTTP/1.1 301 Moved Permanently", etc.
        if (colon == std::string::npos)
        {
            std::vector<std::string> clumps;
            std::string line = cor::Trim(lines[i]);
            if (line.empty())
                continue; // ignore all-whitespace leading line(s)

            cor::Split(line, " ", clumps);
            if (clumps.size() >= 2)
            {
                std::istringstream iss(clumps[1]);
                iss >> mCode;
                if (iss.bad() || iss.fail())
                    throw cor::Exception("Error reading return code in header line '%s'", lines[i].c_str());

                // ignore the '100 Continue' header line
                if (mCode < 200)
                    continue;
            }
            for (size_t j = 2; j < clumps.size(); j++)
            {
                mCodeString += clumps[j] + " ";
            }
            continue;
        }

        // this is a key: value line
        std::string key = line.substr(0, colon);
        // DEFENSIVE: colon should not be final character in line
        if (colon == line.size()-1)
            throw cor::Exception("Error parsing header line '%s'", line.c_str());
        std::string value = line.substr(colon + 1);
        value = cor::Trim(value);

        InsertString(key, value);
    }
}


const struct curl_slist*
HeaderMap::GetCurlHeaders()
{
    // free any existing one
    if (mHeadersCurl != NULL)
        curl_slist_free_all(mHeadersCurl);
    mHeadersCurl = NULL;


    // curl_slist_append returns a new pointer unless there is an error, so
    // a temp variable is required to check for this before updating the
    // pointer. See man page for curl_slist_append().
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        struct curl_slist* temp = curl_slist_append(mHeadersCurl, (i->first + ": " + i->second).c_str());
        if (temp == NULL)
        {
            curl_slist_free_all(mHeadersCurl);
            mHeadersCurl = NULL;
            throw cor::Exception("Http::HeaderMap() -- Error generating headers for packet");
        }
        mHeadersCurl = temp;
    }

    return mHeadersCurl;
}


std::string
DumpCurlHeaders(const struct curl_slist* headers)
{
    std::string s;
    const struct curl_slist* node = headers;
    while (node != NULL)
    {
        s += std::string(node->data) + "\n";
        node = node->next;
    }
    return s;
}

}