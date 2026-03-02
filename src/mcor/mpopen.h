//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_mpopen__
#define __mcor_mpopen__

#include <stdio.h>

#include <mcor/mexception.h>

namespace cor
{

/** class Popen : execute a shell command with either input or output (but
 *  not both; this is a limitation of the underlying popen() function).
 *
 */
class Popen
{
public:
    // indicate direction of pipe, i.e., whether subsequent
    // calls of Read or Write will be made on this object.
    enum Mode { eRead, eWrite };

    Popen(const std::string& command, Mode mode);
	virtual ~Popen();

    // must call Close() in order to check return code via
    // bool() or Assert(), below
    void Close();

	// read until EOF into a string
    // This call will start the command and block until it completes.
    std::string Read();

    // read until newline into a string; returns false when EOF encountered
    // This call will start the command and block until it completes.
    bool Read(std::string& s);

    // write everything into the command pipe
    // This call will start the command and block until it completes.
    void Write(const std::string&);

    operator bool() const { return mRc == 0; }
    void Assert();

private:
    FILE* mFp;
    const std::string mCommand;
    Mode mMode;

    int mRc;

    void Open();

};
    
    
}

#endif
