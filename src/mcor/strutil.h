//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_STRUTIL_H
#define GICM_STRUTIL_H

#include <string>
#include <vector>

namespace cor
{


// Trim whitespace
std::string Trim(const std::string& str,
                 const std::string& whitespace = " \t\n\r");

// Reduce whitespace
//   'asfd    foo' -> 'asdf foo'
//   '   asdf   foo' -> 'asdf foo'
//   'asdf    foo' -> 'asdf----foo' (with fill character '-')
std::string Reduce(const std::string& str,
                   const std::string& fill = " ",
                   const std::string& whitespace = " \t\n\r");

// Split, ignoring adjacent delimiters like foo,,bar or foo//bar
void Split(const std::string& str,
           const std::string& delimiter,
           std::vector<std::string>& results);

// Join
std::string Join(const std::vector<std::string>& v, char delimiter);

// Random
std::string RandomString(size_t length);

// case-insensitive compare
bool IEquals(const std::string& s1, const std::string& s2);

// case-insensitive detection of pattern in source
bool IContains(const std::string& source, const std::string& pattern);

// replace all instances of a substring; returns the count of patterns
// replaced
size_t ReplaceSubstring(std::string& s, const std::string& pattern, const std::string& substitution);

// remove interior iff string is bookended by the given characters
// ex: Bookended(fields[i], "<>", key))
bool Bookended(const std::string& s, const std::string& bookends, std::string& interior);

// convert to all upper-case
std::string ToUpper(const std::string& s);

// convert to all lower-case
std::string ToLower(const std::string& s);

// convert to Camel Case (technically 'start case', see
// https://en.wikipedia.org/wiki/Letter_case) using a delimiter set,
// forcing first character after a delimiter to uppercase and all other
// characters to lowercase.
//
//    "This is a BadIdea Production (TM)"
//    "This Is A Badidea Production (tm)"
//
std::string ToCamel(const std::string& s, const std::string delimiter = " \t\n\r");

// if all alpha
bool IsAlpha(const std::string& s);

// if all alphanumeric plus any of 'valid'
bool IsAlphaNumericPlus(const std::string& s, const std::string& valid);

// convert data to hex string (no '0x' prepended)
std::string ToHex(const void* data, size_t length);

// create hexadecimal dump of data, ala 'hexdump'
std::string HexDump(const void* data, size_t length);
std::string HexDump(const std::vector<unsigned char>& data);

// pad right with character
// result will be at least of size length
std::string PadRight(const std::string&s, char c, size_t length);
// pad left with character
// result will be at least of size length
std::string PadLeft(const std::string&s, char c, size_t length);

// returns whether string matches a '*' or '?' designated glob pattern
// e.g.
//    "I cut and paste from stack exchange"
// compared with
//    "I cut and paste from * exchange"
// returns true
bool GlobMatch(const std::string& s, const std::string& pattern);

// Create K, M, G, T unit string from bytes
std::string HumanReadableMemory(size_t bytes);

// Create a canonical phone number from any format to:
// (XXX) XXX-XXXX
std::string PhoneNumber(const std::string& in);

// Plural units : uses singular unit or non-singular unit along with conversion of
// integer to construct a grammatically correct phrase, ala:
//    1 toaster
//    2 toasters
//    1 dish
//    0 dishes
//    1 syllabus
//    2 syllabi
std::string Plural(size_t count, const std::string& unitSingular, const std::string& unitPlural);

// Construct phrase with commas correct for list, e.g.,
// A, B, and C
// A and B
// A
std::string OxfordList(const std::vector<std::string>& words);

// convert vector to string
void ToString(const std::vector<unsigned char>& v, std::string& s);
void ToString(const std::vector<char>& v, std::string& s);

// convert string to vector
void ToVector(const std::string& s, std::vector<unsigned char>& v);
void ToVector(const std::string& s, std::vector<char>& v);

}
#endif

