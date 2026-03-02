//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_MUUID_MUUID_H
#define GICM_MUUID_MUUID_H

#include <string>
#include <vector>

#include <uuid/uuid.h>

namespace muuid {


/** class uuid : a universally unique ID
 *
 */
class uuid
{
public:
    uuid();
    uuid(const uuid& other);

    void operator=(const uuid& other);

    bool is_nil() const;

    static bool is_valid(const std::string& s);
    static uuid random();

    std::string string() const;
    void set(const std::string& s);
    void set(uuid_t n);

    std::vector<unsigned char> data() const;

    bool less_than(const uuid& other) const;
    bool equal(const uuid& other) const;

private:
    uuid_t mImp;
};

bool operator<(const uuid& u1, const uuid& u2);
bool operator==(const uuid& u1, const uuid& u2);


}

#endif
