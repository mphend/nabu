//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include "serial.h"

#include <mcor/mexception.h>

#include <errno.h>
#include <map>
#include <string.h>
#include <termios.h>
#include <unistd.h>


/* class BaudMap : maps integer baud speeds to enum
 *
 */
class BaudMap : public std::map<int, speed_t>
{
public:
    BaudMap()
    {
        (*this)[9600] = B9600;
        (*this)[19200] = B19200;
        (*this)[38400] = B38400;
        (*this)[115200] = B115200;
    }

    speed_t Get(int baud) const
    {
        const_iterator ci = find(baud);
        if (ci == end())
            throw cor::Exception("Illegal buad rate %d", baud);

        return ci->second;
    }
} baudMap;


namespace cor {
 
Serial::Serial(const std::string& name, int baud) : File(name, O_RDWR),
    mBaud(baud)
{}

Serial::~Serial()
{}

void
Serial::Open()
{
    if (IsOpen())
        return;
    
    speed_t baud = baudMap.Get(mBaud);

    File::Open();
    struct termios settings;
    tcgetattr(GetFd(), &settings);

    // turn off a bunch of old-fashioned terminal nonsense from
    // the primordial UNIX days that try to interpret the stream
    // of data as a VT100 or something.  Echo, CRLF translation, etc.
    cfmakeraw(&settings);

    cfsetospeed(&settings, baud); /* baud rate */
    settings.c_cflag &= ~PARENB; /* no parity */
    settings.c_cflag &= ~CSTOPB; /* 1 stop bit */
    settings.c_cflag &= ~CSIZE;
    settings.c_cflag |= CS8 | CLOCAL; /* 8 bits */
    settings.c_oflag &= ~OPOST; /* raw output */

    tcsetattr(GetFd(), TCSANOW, &settings); /* apply the settings */
    tcflush(GetFd(), TCOFLUSH);
}
    
} // end namespace
