//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_INTERFACE_TYPES_H
#define GICM_NABU_INTERFACE_TYPES_H


namespace nabu
{

// policy on how to add a tag
enum TagPolicy { eMove, eNewOnly, eTemporary };
TagPolicy StringToTagPolicy(const std::string& s);
std::string TagPolicyToString(TagPolicy policy);

// compression types
enum CompressionType { eNoCompression = 0, eZlibCompression = 1 };

// read vs write data access
enum AccessType { eRead, eWrite, eInvalid };

// access policy
// read-write  : reads cannot be simultaneous with writes
// write-mutex : reads can be simultaneous with writes, but writes
//               cannot be simultaneous with other writes
enum AccessPolicy { eReadWrite, eWriteMutex };

// direction of iteration
enum IterationDirection { eIterateForwards, eIterateBackwards };

// record type
//   time-range record
//   point-in-time record
enum RecordType { eTRR, ePITR };

// TRR type
//   event (non-divisible)
//   attribute (divisible)
enum TRRType { eEvent, eAttribute };

// result of waiting for a Select
//    Select returned because an event occurred (eComplete)
//    Select timed out
//    Select should wait again
//    Select was aborted by the system
enum SelectResult { eComplete, eWaitAgain, eTimeout, eAbort };
std::string SelectResultToString(SelectResult r);

struct CopyStats
{
    CopyStats() : mCopied(0), mCompared(0)
    {}
    size_t mCopied;
    size_t mCompared;

    void operator+=(const CopyStats& other);

    std::string Print() const;
};

} // end namespace

#endif
