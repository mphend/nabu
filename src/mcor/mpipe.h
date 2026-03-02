//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef __mcore_pipe__
#define __mcore_pipe__

#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>

#include <mcor/mfile.h>
#include <mcor/mexception.h>

namespace cor
{

/** class Pipe
 *
 *  Creates a named pipe, if it doesn't already exist, and opens with O_RDWR and O_NONBLOCK
 *
 */
class Pipe : public cor::File
{
public:
	Pipe(const std::string& name, mode_t mode);
	virtual ~Pipe();

	// ---- File implementation
	virtual void Open();

private:
    mode_t mMode;
};
    
    
}

#endif
