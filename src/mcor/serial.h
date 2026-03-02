//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcore_serial__
#define __mcore_serial__

#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>

#include <mcor/mfile.h>
#include <mcor/mexception.h>

namespace cor
{

/** class Serial
 *
 *  Manages a serial port, simplified.
 *
 */
class Serial : public cor::File
{
public:
    Serial(const std::string& name, int baudRate);
	virtual ~Serial();

	// ---- File implementation
	virtual void Open();

private:
    int mBaud;
};
    
    
}

#endif
