//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <stdio.h>
#include <linux/limits.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>

#include "mfile.h"
#include "strutil.h"

namespace cor {
std::string
GetHomeDirectory()
{
    uid_t uid = getuid(); // Get the current user's real UID
    struct passwd* pwd = getpwuid(uid);

    if (pwd == NULL)
        throw cor::ErrException("Error in GetHomeDirectory()");

    // SANITY
    if (pwd->pw_dir == NULL)
        throw cor::Exception("GetHomeDirectory() -- empty directory name returned");

    return pwd->pw_dir;
}

 
File::File() : mFd(0), mFlags(0), mCloseFd(true)
{
}

File::File(int fd) : mFd(fd), mFlags(0), mCloseFd(false)
{
}

File::File(const File& other) :
    mPath(other.mPath),
    mFlags(other.mFlags),
    mFd(other.mFd),
    mMode(other.mMode),
    mCloseFd(false)
{
}

File::File(std::string path, int oflags, mode_t mode) :
    mPath(path),
    mFlags(oflags), mMode(mode),
    mFd(0),
    mCloseFd(true)
{
}

File::~File()
{
    Close();
}

bool
File::IsOpen() const
{
    return mFd != 0;
}

bool
File::Exists() const
{
    if (IsOpen())
        return true;

    return (access(mPath.c_str(), F_OK) == 0);
}

bool
File::Exists(const std::string& file)
{
    return (access(file.c_str(), F_OK) == 0);
}

bool
File::IsDirectory(const std::string& file)
{
    struct stat path_stat;
    int rc = stat(file.c_str(), &path_stat);
    if (rc != 0)
    {
        if ((errno == ENOENT) || (errno == ENOTDIR))
            return false;
        throw cor::ErrException("IsDirectory '%s'", file.c_str());
    }
    return S_ISDIR(path_stat.st_mode);
}

bool
File::IsFile(const std::string& file)
{
    struct stat path_stat;
    int rc = stat(file.c_str(), &path_stat);
    if (rc != 0)
    {
        if ((errno == ENOENT) || (errno == ENOTDIR))
            return false;
        throw cor::ErrException("IsFile '%s'", file.c_str());
    }
    return S_ISREG(path_stat.st_mode);
}

// source: https://stackoverflow.com/questions/675039/how-can-i-create-directory-tree-in-c-linux
bool
IsDirExist(const std::string& path)
{
    struct stat info;
    if (stat(path.c_str(), &info) != 0)
    {
        return false;
    }
    return (info.st_mode & S_IFDIR) != 0;
}

bool
makePath(const std::string& path)
{
    mode_t mode = 0755;
    int ret = mkdir(path.c_str(), mode);

    if (ret == 0)
        return true;

    switch (errno)
    {
        case ENOENT:
            // parent didn't exist, try to create it
        {
            int pos = path.find_last_of('/');
            if (pos == std::string::npos)
                return false;
            if (!makePath( path.substr(0, pos) ))
                return false;
        }
            // now, try to create again
            return 0 == mkdir(path.c_str(), mode);

        case EEXIST:
            // done!
            return IsDirExist(path);

        default:
            return false;
    }
}

bool
File::MakePath(const std::string& path)
{
    return makePath(DirName(path));
}

std::string
File::CanonicalPath(const std::string &path, std::vector<std::string> &components)
{
    std::vector<std::string> v;
    cor::Split(path, "/", v);
    bool absolutePath = !path.empty() && (path[0] == '/');

    std::string r;

    for (size_t i = 0; i < v.size(); i++)
    {
        if (!v[i].empty())
        {
            if (v[i] != ".")
            {
                components.push_back(v[i]);
                if (i != 0 || absolutePath)
                    r.append("/");
                r.append(v[i]);
            }
            if (v[i] == "..")
            {
                throw cor::Exception("Relative path in '%s'", path.c_str());
            }
        }
    }

    return r;
}

std::string
File::RelativePath(const std::string& commonRoot, const std::string& path)
{
    std::vector<std::string> commonV;
    CanonicalPath(commonRoot, commonV);

    std::vector<std::string> pathV;
    CanonicalPath(path, pathV);

    if (pathV.size() < commonV.size())
        return path;

    size_t i = 0;
    for (; i < commonV.size(); i++)
    {
        if (pathV[i] != commonV[i])
            break;
    }

    std::string r;
    for ( ; i < pathV.size(); i++)
    {
        r += pathV[i];
        if (i != pathV.size() - 1)
            r += "/";
    }

    return r;
}

std::string
File::DirName(const std::string& path)
{
    if (path.empty())
        return ".";

    // trim everything past the final '/'
    std::string::size_type slash = path.find_last_of('/');
    if (slash == std::string::npos)
        return ".";

    std::string s = path.substr(0, slash);
    std::vector<std::string> v;
    return CanonicalPath(s, v);
}

std::string
File::CWD()
{
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) == NULL)
        throw cor::ErrException("File::CWD");
    return cwd;
}

void
File::FindFilenames(const std::string& pattern, std::vector<std::string>& files)
{
    glob_t globbuf;

    int rc = glob(pattern.c_str(), 0, NULL, &globbuf);

    if (rc == 0) // found something
    {
        for (size_t i=0; i < globbuf.gl_pathc; i++)
        {
            files.push_back(globbuf.gl_pathv[i]);
        }
        globfree(&globbuf);
    }
    else if (rc == GLOB_NOMATCH)
    {
    }
    else if (rc == GLOB_ABORTED)
    {
        throw cor::Exception("Could not read files for '%s'", pattern.c_str());
    }
    else if (rc == GLOB_NOSPACE)
    {
        throw cor::Exception("Out of memory reading files for '%s'", pattern.c_str());
    }
}

void
File::CreateSymlink(const std::string& name, const std::string& path)
{
    // arg 1 is target, arg 2 is linkpath
    // "symlink() creates a symbolic link named linkpath which contains the string target" -- man page
    MakePath(name);
    int rc = symlink(path.c_str(), name.c_str());
    if (rc != 0)
        throw cor::ErrException("Could not create symlink '%s' to '%s'", name.c_str(), path.c_str());
}

std::string
File::FollowSymlink(const std::string& path)
{
    char buffer[PATH_MAX];
    if (readlink(path.c_str(), buffer, sizeof(buffer)) == -1)
        throw cor::ErrException("Could not read symlink target '%s'", path.c_str());
    return buffer;
}

void
File::Rename(const std::string& oldName, const std::string& newName)
{
    int rc = rename(oldName.c_str(), newName.c_str());
    if (rc != 0)
        throw cor::ErrException("Could not rename '%s' to '%s'", oldName.c_str(), newName.c_str());
}

void
File::Open()
{
    if (IsOpen())
        return;

    int fd;
    if (mMode != 0)
        fd = open(mPath.c_str(), mFlags, mMode);
    else
        fd = open(mPath.c_str(), mFlags);

    if (fd == -1)
    {
        throw ErrException("File %s ::Open", mPath.c_str());
    }
    mFd = fd;
}

void
File::Open(int fd)
{
    Close();
    mFd = fd;
}

void
File::Open(std::string path, int oflags)
{
    Close();

    mPath = path;
    mFlags = oflags;

    Open();
}

void
File::Close()
{
    if (!IsOpen())
        return;

    if (!mCloseFd)
        return;

    int rc = close(mFd);
    if (rc == -1)
    {
        throw ErrException("File %s ::Close", mPath.c_str());
    }
    
    mFd = 0;
}

size_t
File::Read(char* buffer, size_t count)
{
    Open();
    ssize_t rc;

    while (true)
    {
        rc = read(mFd, buffer, count);
        if (rc == -1)
        {
            if (errno == EINTR)
                continue;
            throw ErrException("File %s ::Read", mPath.c_str());
        }
        break;
    }

    return rc;
}

size_t
File::Write(const char* buffer, size_t count)
{
    Open();
    ssize_t rc;

    while (true)
    {
        rc = write(mFd, buffer, count);
        if (rc == -1)
        {
            if (errno == EINTR)
            {
                printf("EINTR!\n");
                continue;
            }
           throw ErrException("File %s ::Write", mPath.c_str());
        }
        break;
    }

    return rc;
}

size_t
File::ReadAll(char* buffer, size_t count, double timeoutSeconds)
{
    Open();

    char* p = buffer;
    size_t read = 0;
    while (read < count)
    {
        if (Select(timeoutSeconds, eSelectRead) == 0)
            return read;
        size_t n = Read(p, count - read);
        if (n == 0) // EOF
            return read;
        p += n;
        read += n;
    }
    return read; // unreachable?
}

void
File::ReadAll(std::string& s, size_t count)
{
    Open();

    size_t sz = Size();
    // if count was specified, use it unless it would be larger than the size of
    // the file
    if (count != 0)
    {
        if (count < sz)
            sz = count;
    }
    s.resize(sz);

    size_t read = 0;
    while (read < sz)
    {
        size_t n = Read(&(s[0]) + read, sz - read);
        if (n == 0) // EOF
            return;
        read += n;
    }
}

void
File::WriteAll(const char* buffer, size_t count)
{
    Open();

    const char* p = buffer;
    size_t written = 0;
    while (written < count)
    {
        size_t n = Write(p, count - written);
        p += n;
        written +=n;
    }
}

void
File::Seek(size_t offset, int whence)
{
    Open();

    ssize_t rc = lseek(mFd, offset, whence);
    if (rc == -1)
        throw ErrException("File %s ::Seek", mPath.c_str());
}

size_t
File::Where()
{
    Open();

    ssize_t rc = lseek(mFd, 0, SEEK_CUR);
    if (rc == -1)
        throw ErrException("File %s ::Where", mPath.c_str());
    return rc;
}

size_t
File::Size()
{
    Open();

    off_t here = lseek(mFd, 0, SEEK_CUR);
    if (here == -1)
        throw ErrException("File %s ::Size", mPath.c_str());
    off_t end = lseek(mFd, 0, SEEK_END);
    if (end == -1)
        throw ErrException("File %s ::Size", mPath.c_str());
    off_t rc = lseek(mFd, here, SEEK_SET);
    if (rc == -1)
        throw ErrException("File %s ::Size", mPath.c_str());

    return end;
}

void
File::Sync()
{
    Open();

    int rc = fsync(mFd);
    if (rc == -1)
        throw ErrException("File %s ::Sync", mPath.c_str());
}

void
File::Sync(const std::string& path)
{
    File f(path, O_RDONLY);
    f.Sync();
}

void
File::SyncAll()
{
    ::sync();
}

int
File::Select(double timeoutIn, Selection selection, double* timeLeft)
{
    Open();

    struct timeval timeout;
    if (timeoutIn >= 0)
    {
        timeout.tv_sec = (int)timeoutIn;
        timeout.tv_usec = (timeoutIn - timeout.tv_sec) * 1e6;
    }

    fd_set rfds;
    fd_set wfds;
    fd_set efds;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    FD_SET(mFd, &rfds);
    FD_SET(mFd, &wfds);
    FD_SET(mFd, &efds);
    fd_set* rptr = (selection & eSelectRead) ? &rfds : NULL;
    fd_set* wptr = (selection & eSelectWrite) ? &wfds : NULL;
    fd_set* eptr = (selection & eSelectExcept) ? &efds : NULL;

    // a NULL timeout pointer makes select() block
    struct timeval* toa = &timeout;
    if (timeoutIn < 0)
        toa = NULL;

    int rc = select(mFd + 1, rptr, wptr, eptr, toa);

    if (rc == -1)
    	throw ErrException("File %s ::Select", mPath.c_str());

    if (timeLeft != NULL)
    {
        // timeout now contains the amount of time that remains
        *timeLeft = timeout.tv_usec / 1e6;
        *timeLeft += timeout.tv_sec;
    }
    return rc;
}

int
File::GetFd() const
{
    return mFd;
}

void
File::ForceClose() throw()
{
    try
    {
        Close();
    }  catch (...)
    {
    }
    mFd = 0;
}

void
File::Delete()
{
    if (mPath.empty())
        throw cor::Exception("File::Delete() -- file has no name");

    Close();
    Delete(mPath);
}

bool
File::Delete(const std::string& path)
{
    int rc = unlink(path.c_str());
    if (rc == -1)
    {
        if (errno == ENOENT)
            return false;
        throw ErrException("File %s ::Delete", path.c_str());
    }
    return true;
}

void
File::CopyTo(const std::string& destination)
{
    // get the mode of this file to use on destination
    struct stat path_stat;
    int rc = stat(mPath.c_str(), &path_stat);
    if (rc != 0)
    {
        throw cor::ErrException("CopyTo source file '%s' :", mPath.c_str(), destination.c_str());
    }

    MakePath(destination);
    cor::File other(destination, O_CREAT | O_RDWR | O_TRUNC, path_stat.st_mode);

    const size_t bufSize = 10000;
    char buffer[bufSize];
    size_t n;

    do {
        n = Read(buffer, bufSize);
        if (n == 0)
            break;
        size_t m = other.Write(buffer, n);
        if (m != n)
            throw cor::ErrException("Could not write to '%s':", destination.c_str());
    } while (true);
}

void
File::Rename(const std::string& newName)
{
    if (mPath.empty())
        throw cor::Exception("File::Rename() -- file has no name (new name is %s", newName.c_str());

    int rc = rename(mPath.c_str(), newName.c_str());
    if (rc == -1)
        throw ErrException("File %s ::Rename", mPath.c_str());
    mPath = newName;
}
    
}
