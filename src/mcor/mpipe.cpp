//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include "mpipe.h"

#include <mcor/mexception.h>

#include <errno.h>
#include <unistd.h>
#include <string.h>

namespace cor {
 
Pipe::Pipe(const std::string& name, mode_t mode) : File(name, O_RDWR | O_NONBLOCK),
    mMode(mode)
{}

Pipe::~Pipe()
{}

void
Pipe::Open()
{
    if (IsOpen())
        return;
    
    // try to make fifo, ignoring error if it already exists
    int e = mkfifo(mPath.c_str(), mMode);
    if (e == -1)
    {
        if (errno != EEXIST)
            throw cor::ErrException("Pipe::Open");
    }

    File::Open();
}
    
}
