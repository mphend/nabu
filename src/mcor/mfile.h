//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __threader__mfile__
#define __threader__mfile__

#include <iostream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <typeinfo>
#include <unistd.h>

#include "mexception.h"

namespace cor
{

// return the home directory path, "/users/mike" or "/home/mike", etc.
std::string GetHomeDirectory();


/** class File : a thin wrap of the unix file.
 *
 *  Manages the data associated with opening a file, and does a lazy open.
 *
 */
class File
{
public:
    // will close() on destruction
    File();
    // will not close() on destruction
    File(int fd);
    // will not close() on destruction
    File(const File& other);
    // will close() on destruction
    File(std::string name, int oflags, mode_t mode = S_IRWXU | S_IRWXG | S_IROTH);
    virtual ~File();
    
    virtual bool IsOpen() const;
    virtual bool Exists() const;
    static bool Exists(const std::string& file);
    static bool IsDirectory(const std::string& file);
    static bool IsFile(const std::string& file);

    // create directories up to final '/', e.g.:
    //  foo/bar/baz.txt  create foo and bar
    //  foo/bar/baz.txt/ creates foo, bar, and baz.txt (directories)
    // returns true if path exists or was created, false otherwise
    // Note that if path is just a filename, this returns true
    static bool MakePath(const std::string& path);

    // parse a path with redundant slashes and '.' components into a
    // clean pathname; throws if relative path '..' is encountered
    static std::string CanonicalPath(const std::string& path, std::vector<std::string>& components);

    // return that portion of 'path' that extends past 'commonRoot'
    // if commonRoot is /usr/ubuntu or /usr/ubuntu/
    //    returns bin/foo for /usr/ubuntu/bin/foo
    //    returns bin/bar for /usr/unbutu/bin/bar/
    //    return /opt/usr/ubuntu/bin for /opt/usr/ubuntu/bin
    static std::string RelativePath(const std::string& commonRoot, const std::string& path);

    // return directory potion of path; reentrant version of dirname (3)
    //  foo/bar/baz.txt  returns foo/bar
    //  foo/bar/baz.txt/ returns foo/bar/baz.txt
    static std::string DirName(const std::string& path);

    // return directory potion of path;
    std::string DirName() const { return DirName(GetName()); }

    // return current working directory
    static std::string CWD();

    // do a glob (wildcard) search for files matching pattern, which should
    // include the absolute path, ala:
    // /home/mike/foo/*.txt
    static void FindFilenames(const std::string& pattern, std::vector<std::string>& files);

    // create a symlink at 'name' pointing to 'path'
    static void CreateSymlink(const std::string& name, const std::string& path);

    // get the target of a symlink
    static std::string FollowSymlink(const std::string& path);

    // rename a file
    static void Rename(const std::string& oldName, const std::string& newName);
    
    virtual void Open();
    virtual void Open(int fd);
    virtual void Open(std::string name, int oflags);
    virtual void Close();

    // reads up to count bytes into buffer, looping on EINTR, but otherwise
    // returning the number of bytes read
    virtual size_t Read(char* buffer, size_t count);
    // writes up to count bytes of buffer, looping on EINTR, but otherwise
    // returning the number of bytes written
    virtual size_t Write(const char* buffer, size_t count);

    // loops to read until count bytes are read or until timeoutSeconds elapses
    // in a single read call; returns number of bytes read
    // if timeoutSeconds is negative, this call will block
    virtual size_t ReadAll(char* buffer, size_t count, double timeoutSeconds);
    // loops to read up to 'count' characters from the entire (ordinary) file.
    // If count is zero, the entire file is read
    virtual void ReadAll(std::string& s, size_t count = 0);
    // loops to write the entire buffer
    virtual void WriteAll(const char* buffer, size_t count);
    
    virtual void Seek(size_t offset, int whence);
    // returns current offset
    virtual size_t Where();

    // returns size of ordinary file
    virtual size_t Size();

    // flush this file to disk
    virtual void Sync();
    // sync a named file to disk
    static void Sync(const std::string& path);
    // sync all pending filesystem changes to disk
    static void SyncAll();


    // returns number of ready file descriptors (zero on timeout)
    // if timeoutSeconds is negative, this call will block
    enum Selection { eSelectRead = 0x01, eSelectWrite = 0x02, eSelectExcept = 0x04 };
    virtual int Select(double timeoutSeconds, Selection selection, double* timeLeftSeconds = NULL);

    template <typename T> int Ioctl(unsigned long request, T argp);

    virtual int GetFd() const;
    // try to close, but ignore failures and set fd to zero
    virtual void ForceClose() throw();
    
    // will throw if the File was not constructed with a name (mPath is empty), and Close()
    virtual void Delete();
    // delete the named ordinary file;
    // returns true if a file was deleted, false if there was no such file
    static bool Delete(const std::string& path);

    // copy the ordinary file to destination
    void CopyTo(const std::string& destination);

    // will throw if the File was not constructed with a name (mPath is empty), and Close()
    // renames file, and updates mPath member too
    void Rename(const std::string& newName);

    const std::string& GetName() const { return mPath; }
protected:
    std::string mPath;
    int mFlags;
    mode_t mMode;
    int mFd;
    bool mCloseFd;
};
    
// ==== template implementation
template <typename T>
int
File::Ioctl(unsigned long request, T argp)
{
    Open();

    if (sizeof(T) > sizeof(void*))
        throw cor::Exception("%s: Can't pass arg of type '%s' of size %d through ioctl", mPath.c_str(), sizeof(T), typeid(T).name());

    int rc = ioctl(mFd, request, argp);
    if (rc == -1)
        throw ErrException("File %s ::Ioctl", mPath.c_str());

    return rc;
}
    
}

#endif /* defined(__threader__mfile__) */
