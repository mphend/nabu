//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <cstring>
#include <stdio.h>

#include <mcor/mexception.h>

#include "muuid.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace muuid {

uuid::uuid()
{
    uuid_clear(mImp);
}

uuid::uuid(const uuid& other)
{
    memcpy(mImp, other.mImp, 16);
}

void
uuid::operator=(const uuid& other)
{
    memcpy(mImp, other.mImp, 16);
}

bool
uuid::is_nil() const
{
    return uuid_is_null(mImp);
}

bool
uuid::is_valid(const std::string& s)
{
    uuid_t x;
    return (uuid_parse(s.c_str(), x) == 0);
}

uuid
uuid::random()
{
    uuid r;
    uuid_generate_random(r.mImp);
    return r;
}

std::string
uuid::string() const
{
    char sz[40];
    uuid_unparse_upper(mImp, sz);
    return sz;
}

void
uuid::set(const std::string& s)
{
    if (uuid_parse(s.c_str(), mImp) != 0)
        throw cor::Exception("Invalid uuid string '%s'", s.c_str());
}

void
uuid::set(uuid_t n)
{
    memcpy(mImp, n, 16);
}

std::vector<unsigned char>
uuid::data() const
{
    std::vector<unsigned char> r;
    r.resize(16);
    for (size_t i = 0; i < 16; i++)
        r[i] = mImp[i];
    return r;
}

bool
uuid::less_than(const uuid& other) const
{
    return uuid_compare(mImp, other.mImp) < 0;
}

bool
uuid::equal(const uuid& other) const
{
    return uuid_compare(mImp, other.mImp) == 0;
}

bool
operator<(const uuid& u1, const uuid& u2)
{
    return u1.less_than(u2);
}

bool
operator==(const uuid& u1, const uuid& u2)
{
    return u1.equal(u2);
}


} // end namespace