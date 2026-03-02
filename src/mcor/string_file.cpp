//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <string.h>

#include "string_file.h"

namespace cor
{


size_t StringFile::Read(char* buffer, size_t count)
{
    size_t remaining = mImp.size() - mReadIndex;
    size_t n = count > remaining ? remaining : count;

    strncpy(buffer, mImp.data() + mReadIndex, n);

    mReadIndex += n;

    return n;
}

size_t StringFile::Write(const char* buffer, size_t count)
{
    size_t n = mImp.size();

    // if buffer includes a null-terminator, do not copy it; std::string will
    // handle this internally
    if (buffer[count-1] == 0)
        count--;

    mImp.resize(n + count);
    for (size_t i = 0; i < count; i++)
        mImp[i + n] = buffer[i];

    return mImp.size();
}

}

