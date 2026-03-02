//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_METADATA_MAP_H
#define GICM_NABU_METADATA_MAP_H

#include <fstream>
#include <string>
#include <map>
#include <memory>
#include <set>

#include <mcor/mfile.h>

#include "filenamer.h"
#include "garbage.h"
#include "metadata.h"


namespace nabu
{


/** Metadata keyed by MetaKey, with file based persistence
 *
 */
class MetaDataMapImp : public std::map<MetaKey, MetaData>
{
public:
    MetaDataMapImp();
    MetaDataMapImp(MetaKey::Type type);
    virtual ~MetaDataMapImp();

    void Persist(const std::string& filename) const;
    void PersistDocument(std::string& document) const;
    void Load(const std::string& filename) { LoadImp(filename, false); }
    void LoadDocument(const std::string& document);
    MetaKey::Type GetType() const { return mType; }

    // used for metadata file repair; infers key type from file,
    // and ignores the MD5
    void LoadPermissive(const std::string& filename)  { LoadImp(filename, true); }

    std::string Print() const;

protected:
    typedef std::map<MetaKey, MetaData> Base;

    void PersistDocument(std::ostream& os) const;
    void LoadDocument(std::istream& is, bool permissive);
    void LoadImp(const std::string& filename, bool permissive);

    MetaKey::Type mType;
};

typedef std::shared_ptr<MetaDataMapImp> MetaDataMap;



} // end namespace

#endif
