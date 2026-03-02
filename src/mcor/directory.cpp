//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <dirent.h>

#include <errno.h>
#include "directory.h"
#include "mexception.h"


namespace cor
{


/* class Path implementation
 *
 */

Path::Path(const std::string &pathname)
{
    size_t nSlash = 0;
    while (nSlash != std::string::npos)
    {
        // find next slash
        size_t n = pathname.find_first_of('/', nSlash);
        if (n != std::string::npos)
        {
            std::string el = pathname.substr(nSlash, n - nSlash);
           // special case; if first character is a '/', then store it as a path segment
            if (n == nSlash)
            {
                if (n == 0)
                    mL.push_back("/");
            }
            else
                mL.push_back(el);
            nSlash = n + 1;
        }
        else
        {
            // remaining clump
            mL.push_back(pathname.substr(nSlash, n));
            break;
        }
    }
}

std::string
Path::ToString() const
{
    std::string r;
    std::list<std::string>::const_iterator i = mL.begin();

    while (i != mL.end())
    {
        r += *i;
        i++;
        if (i != mL.end())
            r += "/";
    }

    return r;
}

Path operator+(const Path &path, const std::string &segment)
{
    Path r = path;
    r.mL.push_back(segment);
    return r;
}

std::ostream &operator<<(std::ostream &os, const Path &path)
{
    return (os << path.ToString());
}


/* class Directory implementation
 *
 */

Directory::Directory(const Path& path) : mPath(path), mDirp(0), mD(0)
{

}

Directory::~Directory()
{
    Close();
}

void
Directory::Open()
{
    if (mDirp != 0)
        return;

    mDirp = opendir(mPath.ToString().c_str());
    if (mDirp == NULL)
        throw cor::ErrException("Could not open directory '%s'", mPath.ToString().c_str());
    Next();
}

void
Directory::Close()
{
    if (mDirp != 0)
        closedir(mDirp);
    mDirp = 0;
}

bool
Directory::AtEnd()
{
    Open();
    return mD == 0;
}

bool
Directory::IsDirectory()
{
    Open();
    if (AtEnd())
        throw cor::Exception("Directory :: past end");
    return mD->d_type == DT_DIR;
}


bool
Directory::IsRegularFile()
{
    Open();
    if (AtEnd())
        throw cor::Exception("Directory :: past end");
    return mD->d_type == DT_REG;
}

bool
Directory::Exists()
{
    Open();
    if (AtEnd())
        throw cor::Exception("Directory :: past end");

    Path f = mPath + Name();
    return (access(f.ToString().c_str(), F_OK) == 0);
}

std::string
Directory::Name()
{
    Open();
    if (AtEnd())
        throw cor::Exception("Directory :: past end");
    return mD->d_name;
}

void
Directory::Next()
{
    Open();
    errno = 0;
    mD = readdir(mDirp);
    if (mD == 0)
    {
        if (errno != 0) // error
            throw cor::ErrException("Directory::Next()");
        return; // at end
    }
    std::string name = Name();
    if ((name == ".") || (name == ".."))
        Next();
}

}