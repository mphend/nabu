//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <string.h>

#include <mcor/strutil.h>

#include "hash_md5.h"


namespace cor {

HashValue::HashValue()
{
    Reset();
}

HashValue::HashValue(const HashValue& other)
{
    memcpy(mValue, other.mValue, sizeof(mValue));
}

std::string
HashValue::Print() const
{
    return cor::ToHex(mValue, MD5_DIGEST_LENGTH);
}

void
HashValue::Reset()
{
    memset(mValue, 0, MD5_DIGEST_LENGTH);
}

void
HashValue::operator=(const HashValue& other)
{
    memcpy(mValue, other.mValue, sizeof(mValue));
}

bool
operator<(const HashValue& v1, const HashValue& v2)
{
    //printf("Operator < %s to %s\n", v1.Print().c_str(), v2.Print().c_str());
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
    {
        if (v1.mValue[i] > v2.mValue[i])
        {
            //printf("False\n");
            return false;
        }
        else if (v1.mValue[i] < v2.mValue[i])
        {
            //printf("True\n");
            return true;
        }
        // if equal, continue
    }
    //printf("False\n");
    return false;
}

bool
operator>(const HashValue& v1, const HashValue& v2)
{
    //printf("Operator > %s to %s\n", v1.Print().c_str(), v2.Print().c_str());
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
    {
        if (v1.mValue[i] < v2.mValue[i])
        {
            //printf("False\n");
            return false;
        }
        else if (v1.mValue[i] > v2.mValue[i])
        {
            //printf("True\n");
            return true;
        }
        // if equal, continue
    }
    //printf("False\n");
    return false;
}

bool
operator==(const HashValue& v1, const HashValue& v2)
{
    //printf("Comparing %s to %s\n", v1.Print().c_str(), v2.Print().c_str());
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
    {
        if (v1.mValue[i] != v2.mValue[i])
        {
            //printf("octed %ld is not equal\n", i);
            return false;
        }
    }
    //printf("equal\n");
    return true;
}


Hash::Hash() : mClosed(false)
{
    OpenSSL_add_all_digests();

    const char* method = "md5";
    const EVP_MD *md = EVP_get_digestbyname(method);

    if (md == NULL)
        throw cor::Exception("No such hash method '%s'", method);
    mContext = EVP_MD_CTX_new();

    EVP_DigestInit_ex(mContext, md, NULL);

    //MD5_Init(&mContext);
}

Hash::~Hash()
{
    EVP_MD_CTX_destroy(mContext);
}

void Hash::Add8(uint8_t v)
{
    if (mClosed)
        throw cor::Exception("Cannot Add to closed Hash");
    EVP_DigestUpdate(mContext, &v, 1);
}

void Hash::Add16(uint16_t v)
{
    if (mClosed)
        throw cor::Exception("Cannot Add to closed Hash");
    EVP_DigestUpdate(mContext, &v, 2);
}

void Hash::Add32(uint32_t v)
{
    if (mClosed)
        throw cor::Exception("Cannot Add to closed Hash");
    EVP_DigestUpdate(mContext, &v, 4);
}

void Hash::Add64(uint64_t v)
{
    if (mClosed)
        throw cor::Exception("Cannot Add to closed Hash");
    EVP_DigestUpdate(mContext, &v, 8);
}

void Hash::Add(const std::vector<unsigned char>& v)
{
    if (mClosed)
        throw cor::Exception("Cannot Add to closed Hash");
    EVP_DigestUpdate(mContext, v.data(), v.size());
}

void Hash::Add(const std::string& s)
{
    if (mClosed)
        throw cor::Exception("Cannot Add to closed Hash");
    EVP_DigestUpdate(mContext, s.data(), s.size());
}

HashValue
Hash::Get()
{
    // DigestFinal may only be called once, and Add may not be called afterward
    if (!mClosed)
    {
        unsigned int mdLen;
        EVP_DigestFinal_ex(mContext, mValue.mValue, &mdLen);
        mClosed = true;
    }
    return mValue;
}
    
}
