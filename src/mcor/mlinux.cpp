//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <unistd.h>

#include "mexception.h"


namespace cor
{

int
GetProcessorCount()
{
    int rc = sysconf(_SC_NPROCESSORS_ONLN);

    if (rc < 0)
        throw cor::ErrException("Could not get processor count");

    return rc;
}



}