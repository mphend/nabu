//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_PRIVATE_TYPES_H
#define GICM_NABU_PRIVATE_TYPES_H

#include <string>


namespace nabu
{

// types of files
// 'root' is not actually a type of file, but it is a type of garbage
// entry -- a root metadata node, as for when a tag is deleted
enum FileType { eGarbage, eData, eMetaData, eJournal, eRoot };
std::string FileTypeToString(FileType type);
FileType StringToFileType(const std::string& s);


} // end namespace

#endif
