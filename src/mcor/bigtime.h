//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef MCOR_BIGTIME_H_INCLUDED
#define MCOR_BIGTIME_H_INCLUDED

#include <cstdint>
#include <string>


/** High definition time value
 *
 *  Can represent attosecond accuracy over +/- 29 billion years
 *
 *  An attosecond is 1e-18 seconds
 */
struct BigTime
{
    BigTime();
    BigTime(int64_t seconds, int64_t attoSeconds, bool normalize = true);
    BigTime(double d);
    int64_t mSeconds;
    int64_t mAttoSeconds;

    BigTime& operator +=(BigTime other);
    BigTime& operator -=(BigTime other);
    operator double() const;

    void FromTimeSpec(const struct timespec& ts);
    struct timespec ToTimeSpec() const;

    static BigTime MaxPositive();
    static BigTime MaxNegative();

    void Normalize();

    std::string PrintFractional() const;
    std::string PrintFractionalHex() const;
    // prints seconds and attoseconds as a 32 character string of hex digits
    std::string PrintUnifiedHex() const;
    std::string Print() const;
    std::string HexDump() const;

    void FromUnifiedHex(const std::string& hex);
};

BigTime operator+(BigTime t1, BigTime t2);
BigTime operator-(BigTime t1, BigTime t2);
bool operator==(BigTime t1, BigTime t2);
bool operator>(BigTime t1, BigTime t2);
bool operator>=(BigTime t1, BigTime t2);
bool operator<(BigTime t1, BigTime t2);
bool operator<=(BigTime t1, BigTime t2);


#endif
