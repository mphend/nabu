//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_MCOR_HASH_MD5_INCLUDED
#define PKG_MCOR_HASH_MD5_INCLUDED

#include <openssl/md5.h>
#include <openssl/evp.h>

#include <vector>

#include <mcor/mexception.h>


namespace cor
{


struct HashValue
{
    HashValue();
    HashValue(const HashValue& other);

    unsigned char mValue[EVP_MAX_MD_SIZE];

    size_t Size() const { return EVP_MAX_MD_SIZE; };

    // hexdump
    std::string Print() const;

    // set the value to zero
    void Reset();

    void operator=(const HashValue& other);
};

bool operator<(const HashValue& v1, const HashValue& v2);
bool operator>(const HashValue& v1, const HashValue& v2);
bool operator==(const HashValue& v1, const HashValue& v2);


/** class Hash: an MD5 hash
 *
 *  Be aware md5's are fast but not secure
 *
 */
class Hash
{
public:
    Hash();
    virtual ~Hash();

    // add an 8-bit value to the hash
    void Add8(uint8_t v);
    // add an 16-bit value to the hash
    void Add16(uint16_t v);
    // add an 32-bit value to the hash
    void Add32(uint32_t v);
    // add an 64-bit value to the hash
    void Add64(uint64_t v);
    // add a vector of bytes to the hash
    void Add(const std::vector<unsigned char>& v);
    // add a string to the hash
    void Add(const std::string& s);

    // return the hash
    HashValue Get();

protected:
    bool mClosed;
    EVP_MD_CTX* mContext;
    HashValue mValue;
};
    
}

#endif
