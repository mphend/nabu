//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __pkg_mcor_atfile__
#define __pkg_mcor_atfile__


#include <mcor/mfile.h>


/** class ATFile : enables AT commands on a File interface
 *
 */
class ATFile : public cor::File
{
public:
    ATFile(cor::File& file);
    virtual ~ATFile();

    // send the string and append CR/NL
    void Send(const std::string& send);

    // read until expected string is found, or timeout occurs; if empty string
    // is provided, read until timeout elapses
    std::string Expect(const std::string& expected, double timeout = 1.0);

    // issues an AT command and expects a whitespace-delimited single clump
    // reply terminated by 'OK'; returns the clump only
    std::string Command(const std::string& cmd);
};
#endif
