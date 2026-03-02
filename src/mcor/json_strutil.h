//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_MCOR_JSON_STRUTIL_HPP__
#define __PKG_MCOR_JSON_STRUTIL_HPP__

#include <string>


namespace Json
{

// Escape a string
std::string Escape(const std::string& in);

// Un-escape a string
std::string Unescape(const std::string& in);

}

#endif
