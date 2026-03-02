//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_DIRECTORY_H
#define GICM_DIRECTORY_H


// have to declare this here to avoid confusing clion...
struct dirent;
struct __dirstream;
//typedef __dirstream DIR;

#include <dirent.h>
#include <list>
#include <string>

namespace cor
{


/** class Path : represents a series of path legs, with a possible starting slash
 *
 */

class Path
{
public:
    Path(const std::string& pathname);

    std::string ToString() const;

private:
    friend Path operator+(const Path& path, const std::string& segment);
    std::list<std::string> mL;
};

Path operator+(const Path& path, const std::string& segment);
std::ostream& operator<<(std::ostream& os, const Path& path);


/** class Directory : iterates a directory
 *
 *  Use as such:
 *
 *  cor::Directory dirIter("/opt/data");
 *
 *  while (!dirIter.AtEnd()) {
 *     if (dirIter.IsDirectory())
 *         std::cout << dirIter.Name() << " is a subdirectory" << std::endl;
 *     dirIter++;
 *  }
 */


class Directory
{
public:
    Directory(const Path &path);

    virtual ~Directory();

    // returns true if this is not pointing at anything (it is at end of directory contents)
    bool AtEnd();

    bool IsDirectory();
    bool IsRegularFile();
    bool Exists();

    // XXX due to implementation short-cut, a leading slash will be repeated, e.g.,
    // "//home/mike/data" instead of "/home/mike/data"
    std::string Name();

    // advance to next directory item; might be AtEnd()
    void Next();
    void operator++(int) { Next(); }

private:
    const Path mPath;
    DIR *mDirp;
    struct dirent *mD;

    void Open();
    void Close();
};

}
#endif

