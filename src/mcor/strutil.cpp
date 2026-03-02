//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <map>

#include <ctype.h>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "globmatch.hpp"
#include "mexception.h"
#include "strutil.h"


namespace cor
{

// source: https://stackoverflow.com/questions/1798112/removing-leading-and-trailing-spaces-from-a-string
std::string Trim(const std::string& str,
                 const std::string& whitespace)
{
    const std::string::size_type strBegin = str.find_first_not_of(whitespace);
    if (strBegin == std::string::npos)
        return ""; // no content

    const std::string::size_type strEnd = str.find_last_not_of(whitespace);
    const std::string::size_type strRange = strEnd - strBegin + 1;

    return str.substr(strBegin, strRange);
}

// source: https://stackoverflow.com/questions/1798112/removing-leading-and-trailing-spaces-from-a-string
std::string Reduce(const std::string& str,
                   const std::string& fill,
                   const std::string& whitespace)
{
    // trim first
    std::string result = Trim(str, whitespace);

    // replace sub ranges
    std::string::size_type beginSpace = result.find_first_of(whitespace);
    while (beginSpace != std::string::npos)
    {
        const std::string::size_type endSpace = result.find_first_not_of(whitespace, beginSpace);
        const std::string::size_type range = endSpace - beginSpace;

        result.replace(beginSpace, range, fill);

        const std::string::size_type newStart = beginSpace + fill.length();
        beginSpace = result.find_first_of(whitespace, newStart);
    }

    return result;
}

//source: https://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c
void Split(const std::string& str,
           const std::string& delimiter,
           std::vector<std::string>& results)
{
    size_t pos = 0;
    std::string s = str;
    std::string d = delimiter;
    results.clear();
    while ((pos = s.find(d)) != std::string::npos)
    {
        // ignore adjacent delimiters without adding anything to 'results'
        if (pos != 0)
            results.push_back(s.substr(0, pos));
        s.erase(0, pos + d.length());
    }
    results.push_back(s);
}

std::string
Join(const std::vector<std::string>& v, char delimiter)
{
    if (v.size() == 1)
        return v[0];

    std::string r;
    for (size_t i = 0; i < v.size(); i++)
    {
        r += v[i];
        // note that it is known here that v.size() must be >= 2
        if (i <= v.size() - 2)
            r += delimiter;
    }
    return r;
}

//source: https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
std::string RandomString(size_t length) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(length);

    for (int i = 0; i < length; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return tmp_s;
}

// source: https://stackoverflow.com/questions/11635/case-insensitive-string-comparison-in-c
bool IEquals(const std::string& s1, const std::string& s2)
{
    unsigned int sz = s1.size();
    if (s2.size() != sz)
        return false;
    for (unsigned int i = 0; i < sz; ++i)
        if (tolower(s1[i]) != tolower(s2[i]))
            return false;
    return true;
}

bool
IContains(const std::string& source, const std::string& pattern)
{
    std::string sourceLower = ToLower(source);
    std::string patternLower = ToLower(pattern);
    return sourceLower.find(patternLower) != std::string::npos;
}

// source: https://stackoverflow.com/questions/20406744/how-to-find-and-replace-all-occurrences-of-a-substring-in-a-string
size_t
ReplaceSubstring(std::string& s, const std::string& pattern, const std::string& substitution)
{
    std::string::size_type n = 0;
    size_t count = 0;
    while ((n = s.find(pattern, n)) != std::string::npos)
    {
        count++;
        s.replace(n, pattern.size(), substitution);
        n += substitution.size();
    }

    return count;
}

bool Bookended(const std::string& s, const std::string& bookends, std::string& interior)
{
    if (s.size() < 2)
        return false;

    if (bookends.size() != 2)
        throw cor::Exception("Bookended -- bookends not valid");
    if (s[0] == bookends[0])
    {
        if (s[s.size() - 1] == bookends[bookends.size() - 1])
        {
            interior = s.substr(1,s.length()-2);
            return true;
        }
    }
    return false;
}

std::string
ToUpper(const std::string& s)
{
    std::string r = s;
    for (size_t i = 0; i < r.size(); i++)
        r[i] = toupper(r[i]);

    return r;
}

std::string
ToLower(const std::string& s)
{
    std::string r = s;
    for (size_t i = 0; i < r.size(); i++)
        r[i] = tolower(r[i]);

    return r;
}

std::string
ToCamel(const std::string& str, const std::string delimiter)
{
    size_t pos = 0;
    bool armed = true;

    std::string s = str;
    for (size_t i = 0; i < s.size(); i++)
    {
        if (delimiter.find(s[i]) != std::string::npos)
            armed = true;
        else
        {
            if (armed)
            {
                s[i] = toupper(s[i]);
            }
            else
            {
                s[i] = tolower(s[i]);
            }
            armed = false;
        }
    }
    return s;
}

bool
IsAlpha(const std::string& s)
{
    for (size_t i = 0; i < s.size(); i++)
    {
        if (!isalpha(s[i]))
            return false;
    }
    return true;
}

bool
IsAlphaNumericPlus(const std::string& s, const std::string& valid)
{
    for (size_t i = 0; i < s.size(); i++)
    {
        if ((!isalnum(s[i])) && (valid.find_first_of(s[i]) == std::string::npos))
        {
            return false;
        }
    }
    return true;
}

std::string
ToHex(const void* data, size_t length)
{
    char bufferLine[(length * 2) + 2];

    size_t i;

    size_t n = 0;
    for (i = 0; i < length; ++i)
    {
        n += snprintf(bufferLine + n, sizeof(bufferLine), "%02x", ((unsigned char*)data)[i]);
    }
    bufferLine[n] = 0;
    return bufferLine;
}

// source: https://gist.github.com/ccbrown/9722406, modified to render into a string
std::string
HexDump(const void* data, size_t length)
{
    std::string r;

    char bufferLine[100];
    char ascii[17];
    size_t i, j;
    ascii[16] = '\0';

    size_t n = 0;
    for (i = 0; i < length; ++i)
    {
        n += snprintf(bufferLine + n, sizeof(bufferLine), "%02X ", ((unsigned char*)data)[i]);
        if (((unsigned char*)data)[i] >= ' ' && ((unsigned char*)data)[i] <= '~')
        {
            ascii[i % 16] = ((unsigned char*)data)[i];
        } else
        {
            ascii[i % 16] = '.';
        }
        if ((i+1) % 8 == 0 || i+1 == length)
        {
            n += snprintf(bufferLine + n, sizeof(bufferLine), " ");
            if ((i+1) % 16 == 0)
            {
                n += snprintf(bufferLine + n, sizeof(bufferLine), "|  %s \n", ascii);
                r.append(bufferLine);
                n = 0;
            }
            else if (i+1 == length)
            {
                ascii[(i+1) % 16] = '\0';
                if ((i+1) % 16 <= 8)
                {
                    n += snprintf(bufferLine + n, sizeof(bufferLine), " ");
                }
                for (j = (i+1) % 16; j < 16; ++j)
                {
                    n += snprintf(bufferLine + n, sizeof(bufferLine), "   ");
                }
                n += snprintf(bufferLine + n, sizeof(bufferLine), "|  %s \n", ascii);
                r.append(bufferLine);
                n = 0;
            }
        }
    }

    return r;
}

std::string
PadRight(const std::string& s, char c, size_t length)
{
    if (s.size() >= length)
        return s;

    size_t padSize = length - s.size();
    std::string r = s;
    for (size_t i = 0; i < padSize; i++)
        r += c;
    return r;
}

std::string
PadLeft(const std::string& s, char c, size_t length)
{
    if (s.size() >= length)
        return s;

    size_t padSize = length - s.size();
    std::string r(c, padSize);

    r += s;
    return r;
}

std::string
HexDump(const std::vector<unsigned char>& data)
{
    return HexDump(data.data(), data.size());
}

// source: https://www.partow.net/programming/wildcardmatching/index.html
// also credited in globmatch.hpp
bool GlobMatch(const std::string& s, const std::string& pattern)
{
    return glob::match(s, pattern);
}

class SuffixMap : public std::map<size_t, std::string>
{
public:
    SuffixMap() {
        size_t i = 0;
        (*this)[i++] = "B";
        (*this)[i++] = "KB";
        (*this)[i++] = "MB";
        (*this)[i++] = "GB";
        (*this)[i++] = "TB";
        (*this)[i++] = "PB";
    }
} suffix;

std::string
HumanReadableMemory(size_t bytes)
{
    // source: https://gist.github.com/dgoguerra/7194777

    int i = 0;
    double dblBytes = bytes;

    if (bytes > 1024)
    {
        for (i = 0; (bytes / 1024) > 0 && i < suffix.size() - 1; i++, bytes /= 1024)
            dblBytes = bytes / 1024.0;
    }

    static char output[200];
    snprintf(output, 200, "%.02lf %s", dblBytes, suffix[i].c_str());
    return output;
}

std::string
PhoneNumber(const std::string& in)
{
    std::string out;
    for (std::string::size_type i = 0; i < in.size(); i++)
        if (isdigit(in[i]))
            out.push_back(in[i]);
    if (out.size() < 10)
        throw cor::Exception("PhoneNumber : input '%s' is not a valid phone number (%s)", in.c_str(), out.c_str());
    out.insert(out.begin(), '(');
    out.insert(4, ") ");
    out.insert(9, "-");

    return out;
}

std::string
Plural(size_t count, const std::string& unitSingular, const std::string& unitPlural)
{
    std::string r;
    std::ostringstream oss;
    oss << count << " " << std::string(count == 1 ? unitSingular : unitPlural);

    return oss.str();
}

// A, B, and C
// A and B
// A
std::string
OxfordList(const std::vector<std::string>& words)
{
    std::string r;
    if (words.empty())
        return r;

    // A
    if (words.size() == 1)
        return words.front();

    // A and B
    if (words.size() == 2)
        return words.front() + " and " + words.back();

    // A, B, ..., Y, and Z
    for (size_t i = 0; i < words.size() - 1; i++)
        r += words[i] + ", ";
    r += "and " + words.back();

    return r;
}

void
ToString(const std::vector<unsigned char>& v, std::string& s)
{
    s.resize(v.size());
    memcpy((void*)s.data(), v.data(), v.size());
}

void
ToString(const std::vector<char>& v, std::string& s)
{
    s.resize(v.size());
    memcpy((void*)s.data(), v.data(), v.size());
}

void
ToVector(const std::string& s, std::vector<unsigned char>& v)
{
    v.resize(s.size());
    memcpy((void*)v.data(), s.data(), s.size());
}

void
ToVector(const std::string& s, std::vector<char>& v)
{
    v.resize(s.size());
    memcpy((void*)v.data(), s.data(), s.size());

}

}