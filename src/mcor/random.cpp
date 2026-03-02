//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdlib.h>

#include "random.h"


namespace cor {

// source: https://stackoverflow.com/questions/33058848/generate-a-random-double-between-1-and-1
double Random(double min, double max)
{
    double range = (max - min);
    double div = RAND_MAX / range;
    return min + (rand() / div);
}
    
}
