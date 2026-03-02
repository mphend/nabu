//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_LABEL_FILE__
#define __PKG_NABU_LABEL_FILE__


#include "version.h"

namespace nabu
{


/** class LabelFile
 *
 */

class LabelFile
{
public:
    LabelFile(DatabaseImp& database);
    virtual ~LabelFile();

    void Load();
    void Save();

private:
    DatabaseImp& mDataBase;
    Version mCurrentVersion;
};

}

#endif
