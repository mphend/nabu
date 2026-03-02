//
//  safe_file.h
//
//  Copyright Sun Dog Scientific LLC. All rights reserved.
//

#ifndef __PKG_MCOR_SAFE_FILE__
#define __PKG_MCOR_SAFE_FILE__


#include <stdint.h>

#include <mcor/mfile.h>

namespace cor
{


/** class SafeFile, a class that uses two versioned, CRC validated files to allow
 *  interruption safe writing of what is logically a single file
 *
 */

class SafeFile
{
public:
    // pathGlob must be of form:
    //    any/number/of/directories/something.any.number.of.dots.*.some_extension
    SafeFile(const std::string& pathGlob);
    virtual ~SafeFile();

    // returns whether a file was loaded or not
    bool Load();
    void Save();

    virtual void Parse(cor::File& file, size_t size) = 0;
    virtual void Render(cor::File& file) = 0;

private:
    const std::string mPathGlob;
    uint64_t mCurrentVersion;
    std::string mCurrentFileName;

    uint64_t ExtractVersion(const std::string& fileName);
    std::string ProduceVersion(uint64_t version);
};

}

#endif
