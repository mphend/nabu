//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/mexception.h>
#include "javascript.h"

void
JavascriptObject::Parse(const std::string& s)
{
    if (s.empty()) // defensive
        return;

    // several simplifying assumptions here
    //    1) single nested (one set of curly braces)
    //    2) keys are strings
    //    3) values are single quoted strings with no nested single quotes

    // example: {webVersion:'SEL-3060-R104-0217',deviceType:'Access Point'}
    // assert assumption one
    std::string::size_type startQuote = s.find("{");
    if (startQuote != 0)
        throw cor::Exception("Expected '{' as first character");
    if (s.find("{", 1) != std::string::npos)
        throw cor::Exception("Expected only one '{'");
    if (s.find("}") != s.size() - 1)
        throw cor::Exception("Expected one '}' as last character");

    // iterate over colons
    std::string::size_type nLastDelimiter = 0;
    std::string::size_type nColon = s.find(":");
    while (nColon != std::string::npos)
    {
        // key is from one past last delimiter to one before colon
        std::string key = s.substr(nLastDelimiter + 1, nColon - nLastDelimiter - 1);

        // verify single quote is next
        std::string::size_type nFirstQuote = s.find("'", nColon);
        if (nFirstQuote != nColon + 1)
            throw cor::Exception("Expected single quote to immediately follow colon");
        std::string::size_type nClosingQuote = s.find("'", nFirstQuote + 1);
        if (nClosingQuote == std::string::npos)
            throw cor::Exception("Expected single quote to terminate value");
        std::string value = s.substr(nFirstQuote + 1, nClosingQuote - nFirstQuote - 1);

        // delimiter is comma immediately following closing quote; unless this
        // is the final pair which will return npos
        nLastDelimiter = s.find(",", nClosingQuote);

        // nLastDelimiter will be npos final time
        nColon = s.find(":", nLastDelimiter);
        (*this)[key] = value;
    }
}

std::ostream&
operator<<(std::ostream& oss, const JavascriptObject& js)
{
    JavascriptObject::const_iterator jsci = js.begin();
    for (; jsci != js.end(); jsci++)
    {
        oss << jsci->first << " : " << jsci->second << std::endl;
    }
    return oss;
}
