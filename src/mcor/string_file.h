//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_MCOR_SAFE_FILE__
#define __PKG_MCOR_SAFE_FILE__

#include <string>

#include <mcor/mfile.h>

namespace cor
{


/** class StringFile
 *
 */
class StringFile : public cor::File
{
public:
    StringFile() : mReadIndex(0) {}
    StringFile(const std::string& s) : mImp(s), mReadIndex(0) {}
    void Open() {}

    size_t Read(char* buffer, size_t count);
    size_t Write(const char* buffer, size_t count);

    size_t Size() { return mImp.size(); }
    void Delete() {}
    void Seek(size_t offset, int whence) { throw cor::Exception("Seek unsupported on StringFile"); }
    size_t Where() { return mReadIndex; }
    int Select(double timeoutSeconds, Selection selection, double* timeLeftSeconds = NULL) { return 1; }

    const std::string& String() const { return mImp; }

private:
    size_t mReadIndex;
    std::string mImp;
};

}

#endif
