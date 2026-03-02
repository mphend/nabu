//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <cstdio>
#include <map>
#include <string.h>
#include <stdlib.h>
#include <iostream>

#include <mcor/mpopen.h>
#include <mcor/strutil.h>
#include <ctime>

#include "timespec.h" // note this is a local file

#include "mtime.h"

// an attosecond is 1e-18 seconds
#define ATTOSECONDS_PER_SECOND     1000000000000000000ll
#define ATTOSECONDS_PER_NANOSECOND 1000000000ll

namespace cor {


Time::Time(Initialize init) : mState(eStateInvalid)
{
    if (init == eNow)
    {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        mBigTime.FromTimeSpec(ts);
        mState = eStateOrdinary;
    } else
    {
        mState = eStateInvalid;
    }
}

Time::Time(const time_t &t)
{
    mBigTime.mSeconds = t;
    mBigTime.mAttoSeconds = 0;
    mState = eStateOrdinary;
}

Time::Time(const timespec &t)
{
        mBigTime.FromTimeSpec(t);
        mState = eStateOrdinary;
}

Time::Time(const BigTime& t)
{
    mBigTime = t;
    if (mBigTime == BigTime::MaxPositive())
        mState = eStatePositiveInf;
    else if (mBigTime == BigTime::MaxNegative())
        mState = eStateNegativeInf;
    else
    {
        mBigTime.Normalize();
        mState = eStateOrdinary;
    }
}

Time::Time(int64_t seconds, int64_t attoSeconds)
{
    mBigTime.mSeconds = seconds;
    mBigTime.mAttoSeconds = attoSeconds;
    if (mBigTime == BigTime::MaxPositive())
        mState = eStatePositiveInf;
    else if (mBigTime == BigTime::MaxNegative())
        mState = eStateNegativeInf;
    else
    {
        mBigTime.Normalize();
        mState = eStateOrdinary;
    }
}

Time::Time(const struct tm &t)
{
    struct tm tc = t;
    mBigTime.mSeconds = timegm(&tc);
    mBigTime.mAttoSeconds = 0;
    mState = eStateOrdinary;
}

Time::Time(int year, int month, int day, double second)
{
    struct tm tc;
    tc.tm_year = year - 1900;
    tc.tm_mon = month - 1;
    tc.tm_mday = day;
    tc.tm_sec = tc.tm_min = tc.tm_hour = 0;
    tc.tm_sec = second;

    mBigTime.mSeconds = timegm(&tc);

    // fractional part only
    second -= (long) second;
    mBigTime.mAttoSeconds = second * ATTOSECONDS_PER_SECOND;

    mState = eStateOrdinary;
}

Time::Time(const std::string &t, const char *format)
{
    FromString(t, format);
}

Time::~Time()
{}

Time
Time::PositiveInf()
{
    Time t;
    t.mState = eStatePositiveInf;
    t.mBigTime = BigTime::MaxPositive();
    return t;
}

Time
Time::NegativeInf()
{
    Time t;
    t.mState = eStateNegativeInf;
    t.mBigTime = BigTime::MaxNegative();
    return t;
}

Time
Time::Zero()
{
    return Time((time_t)0);
}

Time
Time::OneSecond()
{
    Time t(1);
    return t;
}

Time
Time::OneMinute()
{
    Time t(60);
    return t;
}

Time
Time::OneHour()
{
    Time t(3600);
    return t;
}

Time
Time::OneDay()
{
    Time t(86400);
    return t;
}

Time
Time::OneWeek()
{
    Time t(7 * 86400);
    return t;
}

Time
Time::OneMonth()
{
    Time t(31 * 86400);
    return t;
}

Time
Time::OneYear()
{
    Time t(365 * 86400);
    return t;
}

bool
Time::Valid() const
{
    return mState != eStateInvalid;
}

bool
Time::IsOrdinary() const
{
    return mState == eStateOrdinary;
}

bool
Time::IsInfinite() const
{
    return IsPositiveInfinite() || IsNegativeInfinite();
}

bool
Time::IsPositiveInfinite() const
{
    return mState == eStatePositiveInf;
}

bool
Time::IsNegativeInfinite() const
{
    return mState == eStateNegativeInf;
}

bool
Time::IsZero() const
{
    return ((mBigTime.mSeconds == 0) && (mBigTime.mAttoSeconds == 0));
}

bool
Time::IsNegative() const
{
    if (IsNegativeInfinite())
        return true;
    else if (IsPositiveInfinite())
        return false;

    // XXX unsure about this
    return ((mBigTime.mSeconds < 0));
}

bool
Time::IsPositive() const
{
    if (IsNegativeInfinite())
        return false;
    else if (IsPositiveInfinite())
        return true;

    // XXX unsure about this
    return ((mBigTime.mSeconds > 0));
}

void
Time::FromString(const std::string &s, const char *format)
{
    static char defaultFormat[] = "%a %b %d %H:%M:%S %Y";

    struct tm tc;
    memset(&tc, 0, sizeof(tm));
    if (format == NULL)
        format = defaultFormat;
    char *c = strptime(s.c_str(), format, &tc);
    if (c == NULL)
        throw cor::Exception("Could not parse string '%s' as time in format %s", s.c_str(), format);
    // is this too strict???  Looking for trailing garbage or whitespace
    if (*c != 0)
        throw cor::Exception("Unexpected trailing digits in '%s' as time in format %s", s.c_str(), format);
    mBigTime.mSeconds = timegm(&tc);
    mBigTime.mAttoSeconds = 0;
    mState = eStateOrdinary;
}

void
Time::TimeOfDayFromString(const std::string &s)
{
    struct tm tc;
    if (strptime(s.c_str(), "%H:%M:%S", &tc) == NULL)
        throw cor::Exception("Could not parse string '%s' as time in format H:M:S", s.c_str());

    // have to pick an arbitrary date: Sat, Jan 1, 2000
    tc.tm_mon = 0;
    tc.tm_mday = 1;
    tc.tm_year = 100;
    mBigTime.mSeconds = timegm(&tc);
    mState = eStateOrdinary;
}

std::string
Time::ToString(const char *format) const
{
    if (!Valid())
        throw cor::Exception("Time::ToString() -- invalid Time object");
    if (IsInfinite())
        throw cor::Exception("Time::ToString() -- time value is infinite");

    char buffer[1000];

    struct tm gmTm = ToTm();
    strftime(buffer, sizeof(buffer), format, &gmTm);
    return buffer;
}

std::string
Time::Date() const
{
    return ToString("%F");
}

std::string
Time::TimeOfDay() const
{
    return ToString("%H:%M:%S");
}

Time
Time::FromDouble(double d)
{
    Time r;
    BigTime bt(d);
    r.mBigTime = bt;
    r.mState = eStateOrdinary;

    return r;
}

double
Time::ToDouble() const
{
    if (!Valid())
        throw cor::Exception("Time::ToDouble() -- invalid Time object");
    if (IsInfinite())
        throw cor::Exception("Time::ToDouble() -- infinite Time object");

    double d = mBigTime.mSeconds;
    d += (double)mBigTime.mAttoSeconds / ATTOSECONDS_PER_SECOND;
    return d;
}

Time &
Time::operator+=(const cor::Time &other)
{
    if (!Valid() || !other.Valid())
        throw cor::Exception("Time::+=() -- invalid Time object");
    if (!IsInfinite())
    {
        mBigTime += other.mBigTime;
    }
    return *this;
}

Time &Time::operator+=(double seconds)
{
    if (!Valid())
        throw cor::Exception("Time::+=() -- invalid Time object");
    if (!IsInfinite())
    {
        BigTime other(seconds);
        mBigTime += other;
    }
    return *this;
}

Time &Time::operator++(int)
{
    if (!Valid())
        throw cor::Exception("Time::++() -- invalid Time object");
    if (!IsInfinite())
    {
        mBigTime += BigTime(1,0);
    }
    return *this;
}

Time &Time::operator++()
{
    return (*this)++;
}

Time &
Time::operator-=(const cor::Time &other)
{
    if (!Valid() || !other.Valid())
        throw cor::Exception("Time::-=() -- invalid Time object");
    if (!IsInfinite())
    {
        mBigTime -= other.mBigTime;
    }
    return *this;
}

Time &Time::operator-=(double seconds)
{
    if (!Valid())
        throw cor::Exception("Time::-=() -- invalid Time object");
    if (!IsInfinite())
    {
        BigTime other(seconds);
        mBigTime -= other;
    }
    return *this;
}

Time &Time::operator--(int)
{
    if (!Valid())
        throw cor::Exception("Time::--() -- invalid Time object");
    if (!IsInfinite())
    {
        mBigTime -= BigTime(1,0);
    }
    return *this;
}

Time &Time::operator--()
{
    return (*this)--;
}

Time&
Time::operator *=(const cor::Time& other)
{
    if (!Valid() || !other.Valid())
        throw cor::Exception("Time::*=() -- invalid Time object");

    if (other.IsZero())
    {
        *this = Zero();
    }
    else if (IsNegativeInfinite())
    {
        if (other.IsNegativeInfinite() || other.IsNegative())
            *this = PositiveInf();
        // all other cases this remains neg inf
    }
    else if (IsPositiveInfinite())
    {
        if (other.IsNegativeInfinite() || other.IsNegative())
            *this = NegativeInf();
        // all other cases this remains pos inf
    }
    else if (other.IsNegativeInfinite())
    {
        if (IsNegative())
            *this = PositiveInf();
    }
    else if (other.IsPositiveInfinite())
    {
        if (IsNegative())
            *this = NegativeInf();
    }
    else
    {
        // both are ordinary, non-zero
        /*
        struct timespec product = mTimespec;

        product.tv_sec *= other.mBigTime.mSeconds;

        product.tv_nsec *= other.mTimespec.tv_nsec;
        product.tv_nsec += (mBigTime.mSeconds * other.mTimespec.tv_nsec);
        product.tv_nsec += (mTimespec.tv_nsec * other.mBigTime.mSeconds);

        mTimespec = product;
        timespec_normalise(mTimespec);
        */
        // XXX afraid of sign errors introduced by overflow; just
        // do this with floating point math
        double me = ToDouble();
        double them = other.ToDouble();
        *this = FromDouble(me * them);
    }

    return *this;
}

Time&
Time::operator *=(double seconds)
{
    return (*this) *= cor::Time(seconds);
}

Time&
Time::operator /=(const cor::Time& other)
{
    if (other.IsZero())
        throw cor::Exception("Time::operator /=() divide-by-zero");

    return (*this) *= FromDouble(1.0 / other.ToDouble());
}

Time&
Time::operator /=(double seconds)
{
    if (seconds == 0)
        throw cor::Exception("Time::operator /=() divide-by-zero");

    return (*this) *= FromDouble(1.0 / seconds);
}

struct tm
Time::ToTm() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::ToTm() called on invalid/infinite Time object");


    struct tm t;
    time_t l = mBigTime.mSeconds;
    gmtime_r(&l, &t);
    return t;
}

/*
      struct tm {
            int tm_sec;    // Seconds (0-60)
            int tm_min;    // Minutes (0-59)
            int tm_hour;   // Hours (0-23)
            int tm_mday;   // Day of the month (1-31)
            int tm_mon;    // Month (0-11)
            int tm_year;   // Year - 1900
            int tm_wday;   // Day of the week (0-6, Sunday = 0)
            int tm_yday;   // Day in the year (0-365, 1 Jan = 0)
            int tm_isdst;  // Daylight saving time
       };
*/

int
Time::HourOfDay() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::HourOfDay() called on invalid/infinite Time object");

    return ToTm().tm_hour;
}

void
Time::SetHourOfDay(int hourOfDay)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SetHourOfDay() called on invalid/infinite Time object");

    struct tm tc = ToTm();
    tc.tm_hour = hourOfDay;

    mBigTime.mSeconds = timegm(&tc);

}

int
Time::Minute() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Minute() called on invalid/infinite Time object");

    return ToTm().tm_min;
}

void
Time::SetMinute(int minute)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SetMinute() called on invalid/infinite Time object");

    struct tm tc = ToTm();
    tc.tm_min = minute;

    mBigTime.mSeconds = timegm(&tc);
}

int
Time::Second() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Second() called on invalid/infinite Time object");

    return ToTm().tm_sec;
}

void
Time::SetSecond(int second)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SetSecond() called on invalid/infinite Time object");


    struct tm tc = ToTm();
    tc.tm_sec = second;

    mBigTime.mSeconds = timegm(&tc);
}

int
Time::SecondOfDay() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SecondOfDay() called on invalid/infinite Time object");

    return HourOfDay() * 3600 + Minute() * 60 + Second();
}

void
Time::SetSecondOfDay(int second)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SetSecondOfDay() called on invalid/infinite Time object");

    struct tm tc = ToTm();
    tc.tm_hour = tc.tm_min = 0;
    tc.tm_sec = second;

    mBigTime.mSeconds = timegm(&tc);
    mBigTime.mAttoSeconds = 0;
}

int
Time::DayOfWeek() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::DayOfWeek() called on invalid/infinite Time object");

    return ToTm().tm_wday;
}

class DOWMap : public std::map<int, std::string>
{
public:
    DOWMap()
    {
        (*this)[0] = "Sunday";
        (*this)[1] = "Monday";
        (*this)[2] = "Tuesday";
        (*this)[3] = "Wednesday";
        (*this)[4] = "Thursday";
        (*this)[5] = "Friday";
        (*this)[6] = "Saturday";
    }
} dowMap;

void
Time::DayOfWeek(std::string &dow)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::DayOfWeek() called on invalid/infinite Time object");

    dow = dowMap.at(DayOfWeek());
}

int
Time::Day() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Day() called on invalid/infinite Time object");

    return ToTm().tm_mday;
}

void
Time::SetDay(int dayOfMonth)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SetDay() called on invalid/infinite Time object");

    struct tm tc = ToTm();
    tc.tm_mday = dayOfMonth;

    mBigTime.mSeconds = timegm(&tc);
}

int
Time::WeekOfMonth() const
{
    // week 1 starts on the 1st day of the month
    // week 2 starts on the first Sunday after that
    // etc.

    int thisDayOfMonth = Day();

    cor::Time r = *this;
    r.SetDay(1);
    r = r.NextWeek();
    // r is now first Sunday of the month

    // handle case that month starts on a Sunday
    if (r.Day() == 1)
    {
        if (thisDayOfMonth == 1)
            return 1;
        r = (r + OneSecond()).NextWeek();
    }
    // r is now first Sunday after day 1
    const size_t maxWeeksInAMonth = 6;
    for (size_t w = 1; w <= maxWeeksInAMonth; w++)
    {
        if (*this < r)
            return w;
        r = (r + OneSecond()).NextWeek();
    }

    // this is impossible and non-sensical
    throw cor::Exception("Time::WeekOfMonth() -- %d is not in the month containing %s\n", thisDayOfMonth, Date().c_str());
}

int
Time::DayOfMonth() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::DayOfMonth() called on invalid/infinite Time object");

    return ToTm().tm_mday; // 1-based
}

int
Time::Month() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Month() called on invalid/infinite Time object");

    return ToTm().tm_mon + 1;
}

void
Time::SetMonth(int month)
{
    if (!IsOrdinary())
        throw cor::Exception("Time::SetMonth() called on invalid/infinite Time object");

    struct tm tc = ToTm();
    tc.tm_mon = month;

    mBigTime.mSeconds = timegm(&tc);
}

int
Time::Year() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Year() called on invalid/infinite Time object");

    return ToTm().tm_year + 1900;
}

int
Time::DayOfYear() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::DayOfYear() called on invalid/infinite Time object");

    return ToTm().tm_yday + 1;
}

double
Time::Fraction() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Fraction() called on invalid/infinite Time object");

    double r = mBigTime;
    return r - ((long) r);
}

bool
Time::SameDay(const cor::Time& other) const
{
    if (!IsOrdinary() || !other.IsOrdinary())
        throw cor::Exception("Time::SameDay called on non-ordinary time");

    if (Day() != other.Day())
        return false;
    if (Month() != other.Month())
        return false;
    return (Year() == other.Year());
}

Time
Time::TopOfSecond(eSnapDirection direction, int quantity) const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::TopOfSecond() called on invalid/infinite Time object");

    if (direction == eBackwards)
        quantity = -quantity;
    else
        quantity++;

    struct tm t;
    time_t l = mBigTime.mSeconds;
    localtime_r(&l, &t);

    t.tm_sec += quantity;
    time_t s = mktime(&t);

    struct timespec ts;
    ts.tv_sec = s;
    ts.tv_nsec = 0;

    return Time(ts);
}

Time
Time::TopOfMinute(eSnapDirection direction, int quantity) const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::TopOfMinute() called on invalid/infinite Time object");

    if (direction == eBackwards)
        quantity = -quantity;
    else
        quantity++;

    struct tm t;
    time_t l = mBigTime.mSeconds;
    localtime_r(&l, &t);

    t.tm_min += quantity;
    t.tm_sec = 0;
    time_t s = mktime(&t);

    struct timespec ts;
    ts.tv_sec = s;
    ts.tv_nsec = 0;

    return Time(ts);
}

Time
Time::TopOfHour(eSnapDirection direction, int quantity) const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::TopOfHour() called on invalid/infinite Time object");

    if (direction == eBackwards)
        quantity = -quantity;
    else
        quantity++;

    struct tm t;
    time_t l = mBigTime.mSeconds;
    localtime_r(&l, &t);

    t.tm_hour += quantity;
    t.tm_min = 0;
    t.tm_sec = 0;
    time_t s = mktime(&t);

    struct timespec ts;
    ts.tv_sec = s;
    ts.tv_nsec = 0;

    return Time(ts);
}

Time
Time::TopOfDay(eSnapDirection direction, int quantity) const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::TopOfDay() called on invalid/infinite Time object");

    if (direction == eBackwards)
        quantity = -quantity;
    else
        quantity++;

    struct tm t;
    time_t l = mBigTime.mSeconds;
    localtime_r(&l, &t);

    struct tm g;
    gmtime_r(&l, &g);

    t.tm_mday += quantity;
    t.tm_hour = t.tm_hour - g.tm_hour;

    t.tm_min = 0;
    t.tm_sec = 0;
    time_t s = mktime(&t);

    struct timespec ts;
    ts.tv_sec = s;
    ts.tv_nsec = 0;

    return Time(ts);
}

std::string
Time::Print(bool decimal) const
{
    if (!Valid())
        return "invalid";
    if (IsPositiveInfinite())
        return "+inf";
    if (IsNegativeInfinite())
        return "-inf";

    struct tm t;
    time_t l = mBigTime.mSeconds;
    gmtime_r(&l, &t);

    // final output is in form 'Fri Dec 31 18:08:14.000064007 2021'

    char buffer[500];
    // want 'Fri Dec 31 18:08:14'; year appended after fractional part
    // of seconds
    if (strftime(buffer, sizeof(buffer), "%a %b %d %H:%M:%S", &t) == 0)
        throw cor::Exception("Time:: Could not render time (buffer too small)\n"); // DEFENSIVE
    std::string s(buffer);

    // fractional part of second, if desired
    if (decimal)
    {
        snprintf(buffer, sizeof(buffer), ".%018lld", (long long)mBigTime.mAttoSeconds);
        s += std::string(buffer);
    }

    // year
    if (strftime(buffer, sizeof(buffer), " %Y", &t) == 0)
        throw cor::Exception("Time:: Could not render time (buffer too small)\n"); // DEFENSIVE
    s += std::string(buffer);

    return s;
}

void
Time::Print(std::ostream &os, bool decimal) const
{
    os << Print(decimal);
}

std::string
Time::HexDump() const
{
    return mBigTime.PrintUnifiedHex();
}

void
Time::FromHexDump(const std::string& hex)
{
    mBigTime.FromUnifiedHex(hex);
}

void
Time::Round()
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Round() called on invalid/infinite Time object");

    if (mBigTime.mAttoSeconds >= 500000000000000ll) // 5 with 14 zeros
        mBigTime.mSeconds++;
    mBigTime.mAttoSeconds = 0;
}

void
Time::Floor()
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Floor() called on invalid/infinite Time object");

    mBigTime.mAttoSeconds = 0;
}

void
Time::Ceiling()
{
    if (!IsOrdinary())
        throw cor::Exception("Time::Ceiling() called on invalid/infinite Time object");

    if (mBigTime.mAttoSeconds != 0)
    {
        mBigTime.mSeconds++;
        mBigTime.mAttoSeconds = 0;
    }
}

Time
Time::NextHour() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::NextHour() called on invalid/infinite Time object");

    time_t x = mBigTime.mSeconds;
    x = x % cor::Time::OneHour().Seconds();
    if (x != 0)
        return mBigTime.mSeconds + cor::Time::OneHour().Seconds() - x;

    // already at top-of-hour
    return mBigTime.mSeconds;
}

Time
Time::NextHourOfDay(int hourOfDay) const
{
    if (hourOfDay >= HoursPerDay())
        throw cor::Exception("Time::NextHourOfDay() -- value '%d' out of range", hourOfDay);
    if (!IsOrdinary())
        throw cor::Exception("Time::NextHourOfDay() called on invalid/infinite Time object");

    Time r = NextHour();

    int delta = hourOfDay - r.HourOfDay();
    if (delta < 0) // in the past; go to next day
        r += (HoursPerDay() + delta) * OneHour();
    else
        r += delta * OneHour();

    return r;
}

Time
Time::NextDay() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::NextDay() called on invalid/infinite Time object");

    time_t x = mBigTime.mSeconds;
    x = x % cor::Time::OneDay().Seconds();
    if (x != 0)
        return mBigTime.mSeconds + cor::Time::OneDay().Seconds() - x;
    return mBigTime.mSeconds;
}

Time
Time::NextDayOfWeek(int dayOfWeek) const
{
    if (dayOfWeek >= DaysPerWeek())
        throw cor::Exception("Time::NextDayOfWeek() -- value '%d' out of range", dayOfWeek);
    if (!IsOrdinary())
        throw cor::Exception("Time::NextDayOfWeek() called on invalid/infinite Time object");
    Time r = NextDay();

    int delta = dayOfWeek - r.DayOfWeek();
    if (delta < 0) // in the past; go to next day
        r += (DaysPerWeek() + delta) * OneDay();
    else
        r += delta * OneDay();

    return r;
}

Time
Time::NextDayOfMonth(int dayOfMonth) const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::NextDayOfMonth() called on invalid/infinite Time object");

    Time r = NextDay();

    if (r.DayOfMonth() == dayOfMonth)
        return r;

    // if past this day of the month, use next month
    if (r.DayOfMonth() >= dayOfMonth)
    {
        cor::Time t = Time(r.Year(), r.Month() + 1, dayOfMonth, 0);
        // check to make sure t is in next month
        if (t.Month() != r.Month() + 1)
        {
            // went too far (because dayOfMonth > days in next month);
            // snap back to final day of month
            t.SetMonth(r.Month() + 1);
            t.SetDay(1);
            t -= OneDay();
        }
        return t;
    }

    cor::Time t = Time(r.Year(), r.Month(), dayOfMonth, 0);
    if (t.Month() != r.Month())
    {
        // went too far (because dayOfMonth > days in this month);
        // snap back to final day of month
        t.SetMonth(r.Month());
        t.SetDay(1);
        t -= OneDay();
    }
    return t;
}

Time
Time::NextMonth() const
{
    if (!IsOrdinary())
        throw cor::Exception("Time::NextMonth() called on invalid/infinite Time object");

    Time r = NextDay();
    if (r.DayOfMonth() == 0)
        return r;
    // note that day-of-month is 1 based, as is month
    if (r.Month() == 12)
    {
        // first day of January, next year
        return Time(r.Year() + 1, 1, 1, 0);
    }

    // first day of next month, this year
    return Time(r.Year(), r.Month() + 1, 1, 0);
}

Time
Time::NextMonthOfYear(int monthOfYear) const
{
    if (monthOfYear > 12)
        throw cor::Exception("Time::NextMonthOfYear() -- value '%d' out of range", monthOfYear);
    if (!IsOrdinary())
        throw cor::Exception("Time::NextMonthOfYear() called on invalid/infinite Time object");

    Time r = NextDay();
    if ((r.Month() == monthOfYear) && (r.DayOfMonth() == 0))
        return r;

    if (r.Month() >= monthOfYear)
    {
        return Time(r.Year() + 1, monthOfYear, 1, 0);
    }
    return Time(r.Year(), monthOfYear, 1, 0);
}

void
Time::Scale(double factor)
{
    if (!Valid())
        throw cor::Exception("Scale() called on invalid Time object");
    if (IsInfinite())
    {
        if (factor == 0)
        {
            mBigTime.mSeconds = mBigTime.mAttoSeconds = 0;
        }
    }
    else
    {
        mBigTime.mSeconds *= factor;
        mBigTime.mAttoSeconds *= factor;
        mBigTime.Normalize();
    }
}

Time
Time::Average(const Time& other) const
{
    if (!IsOrdinary() || !other.IsOrdinary())
        throw cor::Exception("Time::Average() called on invalid/infinite Time object");

    BigTime r = mBigTime + other.mBigTime;
    r.mSeconds /= 2;
    r.mAttoSeconds /= 2;
    r.Normalize();

    Time t;
    t.mBigTime = r;
    t.mState = eStateOrdinary;
    return t;
}

int64_t
Time::SecondPortion() const
{
    if (!Valid())
        throw cor::Exception("Time::SecondPortion() called on invalid Time object");

    if (IsPositiveInfinite())
        return BigTime::MaxPositive().mSeconds;
    if (IsNegativeInfinite())
        return BigTime::MaxNegative().mSeconds;

    // ordinary
    return mBigTime.mSeconds;
}

int64_t
Time::AttoSecondPortion() const
{
    if (!Valid())
        throw cor::Exception("Time::AttoSecondPortion() called on invalid Time object");

    if (IsPositiveInfinite())
        return BigTime::MaxPositive().mAttoSeconds;
    if (IsNegativeInfinite())
        return BigTime::MaxNegative().mAttoSeconds;

    // ordinary
    return mBigTime.mAttoSeconds;
}

int32_t
Time::AttoSecondToNanoSecond(int64_t asec)
{
    return asec / ATTOSECONDS_PER_NANOSECOND;
}

/*int64_t
Time::NanoSecondPortion() const
{
    // nanoseconds cannot represent infinite time value; use attoseconds
    // instead
    if (!IsOrdinary())
        throw cor::Exception("Time::NanoSecondPortion() called on invalid/infinite Time object");
    //printf("foo: %lu\n", mBigTime.mAttoSeconds);
    return mBigTime.mAttoSeconds / ATTOSECONDS_PER_NANOSECOND;
}*/

int
Time::Seconds() const
{
    return SecondPortion();
}

int
Time::Minutes() const
{
    return SecondPortion() / 60;
}

int
Time::Hours() const
{
    return SecondPortion() / 3600;
}

int
Time::Days() const
{
    return SecondPortion() / 86400;
}

int
Time::Weeks() const
{
    return SecondPortion() / (86400 * 7);
}

int
Time::Months() const
{
    return SecondPortion() / (86400 * 30);
}

int
Time::Years() const
{
    return SecondPortion() / (86400 * 365);
}

std::string
Time::DurationString(size_t unitMax) const
{
    std::string r;

    cor::Time n = Seconds();
    size_t conversions = 0;

    // Years
    size_t years = n.Years();
    if (years)
    {
        r = Plural(years, "year", "years");
        conversions++;
        n = n.Seconds() % (OneYear().Seconds());
    }
    if (unitMax && conversions == unitMax)
        return r;

    // Months
    size_t months = n.Months();
    if (months)
    {
        if (conversions != 0)
            r += ", ";
        r += Plural(months, "month", "months");
        conversions++;
        n = n.Seconds() % (OneMonth().Seconds());
    }
    if (unitMax && conversions == unitMax)
        return r;

    // Weeks
    size_t weeks = n.Weeks();
    if (weeks)
    {
        if (conversions != 0)
            r += ", ";
        r += Plural(weeks, "week", "weeks");
        conversions++;
        n = n.Seconds() % (OneWeek().Seconds());
    }
    if (unitMax && conversions == unitMax)
        return r;

    // Days
    size_t days = n.Days();
    if (days)
    {
        if (conversions != 0)
            r += ", ";
        r += Plural(days, "day", "days");
        conversions++;
        n = n.Seconds() % (OneDay().Seconds());
    }
    if (unitMax && conversions == unitMax)
        return r;

    // Hours
    size_t hours = n.Hours();
    if (hours)
    {
        if (conversions != 0)
            r += ", ";
        r += Plural(hours, "hour", "hours");
        conversions++;
        n = n.Seconds() % (OneHour().Seconds());
    }
    if (unitMax && conversions == unitMax)
        return r;

    // Minutes
    size_t minutes = n.Minutes();
    if (minutes)
    {
        if (conversions != 0)
            r += ", ";
        r += Plural(minutes, "minute", "minutes");
        conversions++;
        n = n.Seconds() % (OneMinute().Seconds());
    }
    if (unitMax && conversions == unitMax)
        return r;

    // Seconds
    if (n.Seconds())
    {
        if (conversions != 0)
            r += ", ";
        r += Plural(n.Seconds(), "second", "seconds");
    }

    if (conversions == 0)
        r = "0 seconds";

    return r;
}

Time
operator +(const Time& t, double seconds)
{
    if (!t.Valid())
        throw cor::Exception("Time::operator+ called on invalid Time object");
    if (t.IsOrdinary())
    {
        Time r = t;
        r += seconds;
        return r;
    }
    return t;
}

Time
operator +(double seconds, const Time& t)
{
    return t + seconds;
}

Time
operator +(const Time& t1, const Time& t2)
{
    if (!t1.Valid() || !t2.Valid())
        throw cor::Exception("Time::operator+ called on invalid Time object");

    // contrary infinite
    if ((t1.IsPositiveInfinite() && t2.IsNegativeInfinite()) ||
        (t1.IsNegativeInfinite() && t2.IsPositiveInfinite()))
        return Time(Time::eInvalid);

    // one is infinite
    if (t1.IsInfinite())
        return t1;
    if (t2.IsInfinite())
        return t2;

    // both ordinary
    return timespec_add(t1.ToTimeSpec(), t2.ToTimeSpec());
}

Time
operator *(const Time& t, double d)
{
    if (!t.Valid())
        throw cor::Exception("Time::operator* called on invalid Time object");

    if (d == 0)
        return Time(0);

    cor::Time r = t;
    r.Scale(d);

    return r;
}

Time operator *(double d,const Time& t)
{
    return t * d;
}

Time operator *(const Time& t, int d)
{
    return (double)d * t;
}

Time operator *(int d,const Time& t)
{
    return (double)d * t;
}

Time
operator -(const Time& t, double seconds)
{
    Time r = t;
    r -= seconds;
    return r;
}

Time
operator -(double seconds, const Time& t)
{
    return cor::Time(seconds) - t;
}

Time
operator -(const Time& t1, const Time& t2)
{
    if (!t1.Valid() || !t2.Valid())
        throw cor::Exception("Time::operator- called on invalid Time object");

    // contrary infinite
    if ((t1.IsPositiveInfinite() && t2.IsNegativeInfinite()) ||
        (t1.IsNegativeInfinite() && t2.IsPositiveInfinite()))
        return Time(Time::eInvalid);

    // one is infinite
    if (t1.IsInfinite())
        return t1;
    if (t2.IsInfinite())
        return t2;

    // both ordinary
    return timespec_sub(t1.ToTimeSpec(), t2.ToTimeSpec());
}

bool
operator >(const Time& t1, const Time& t2)
{
    if (!t1.Valid() || !t2.Valid())
        throw cor::Exception("Time::operator> called on invalid Time object");

    // identical infinite
    if (t1.IsPositiveInfinite() && t2.IsPositiveInfinite())
        return false;
    if (t1.IsNegativeInfinite() && t2.IsNegativeInfinite())
        return false;

    // only one is infinite, or both are contrary infinite
    if (t1.IsPositiveInfinite() || t2.IsNegativeInfinite())
        return true;
    if (t2.IsPositiveInfinite() || t1.IsNegativeInfinite())
        return false;

    // both ordinary
    return timespec_gt(t1.ToTimeSpec(), t2.ToTimeSpec());
}

bool
operator >=(const Time& t1, const Time& t2)
{
    if (!t1.Valid() || !t2.Valid())
        throw cor::Exception("Time::operator>= called on invalid Time object");

    // identical infinite
    if (t1.IsPositiveInfinite() && t2.IsPositiveInfinite())
        return true;
    if (t1.IsNegativeInfinite() && t2.IsNegativeInfinite())
        return true;

    // only one is infinite, or both are contrary infinite
    if (t1.IsPositiveInfinite() || t2.IsNegativeInfinite())
        return true;
    if (t2.IsPositiveInfinite() || t1.IsNegativeInfinite())
        return false;

    // both ordinary
    return timespec_ge(t1.ToTimeSpec(), t2.ToTimeSpec());
}

bool
operator <(const Time& t1, const Time& t2)
{
    if (!t1.Valid() || !t2.Valid())
        throw cor::Exception("Time::operator< called on invalid Time object");

    // identical infinite
    if (t1.IsPositiveInfinite() && t2.IsPositiveInfinite())
        return false;
    if (t1.IsNegativeInfinite() && t2.IsNegativeInfinite())
        return false;

    // only one is infinite, or both are contrary infinite
    if (t1.IsPositiveInfinite() || t2.IsNegativeInfinite())
        return false;
    if (t2.IsPositiveInfinite() || t1.IsNegativeInfinite())
        return true;

    // both ordinary
    return timespec_lt(t1.ToTimeSpec(), t2.ToTimeSpec());
}

bool
operator <=(const Time& t1, const Time& t2)
{
    if (!t1.Valid() || !t2.Valid())
        throw cor::Exception("Time::operator<= called on invalid Time object");

    // identical infinite
    if (t1.IsPositiveInfinite() && t2.IsPositiveInfinite())
        return true;
    if (t1.IsNegativeInfinite() && t2.IsNegativeInfinite())
        return true;

    // only one is infinite, or both are contrary infinite
    if (t1.IsPositiveInfinite() || t2.IsNegativeInfinite())
        return false;
    if (t2.IsPositiveInfinite() || t1.IsNegativeInfinite())
        return true;

    // both ordinary
    return timespec_le(t1.ToTimeSpec(), t2.ToTimeSpec());
}

bool
operator ==(const Time& t1, const Time& t2)
{
    // both invalid
    if (!t1.Valid() && !t2.Valid())
        return true;
    // only one invalid
    if (!t1.Valid() || !t2.Valid())
        return false;

    // identical infinite
    if (t1.IsPositiveInfinite() && t2.IsPositiveInfinite())
        return true;
    if (t1.IsNegativeInfinite() && t2.IsNegativeInfinite())
        return true;

    // only one is infinite, or both are contrary infinite
    if (t1.IsInfinite() || t2.IsInfinite())
        return false;

    // both ordinary
    //return timespec_eq((struct timespec)t1, (struct timespec)t2);
    return t1.ToBigTime() == t2.ToBigTime();
}

bool
operator !=(const Time& t1, const Time& t2)
{
    return !(t1 == t2);
}

std::ostream&
operator<<(std::ostream& os, const Time& t)
{
    t.Print(os);
    return os;
}


LocalTime::LocalTime(const std::string& timezone) :
    Time(),
    mTimezone(timezone)
{
    SetTimezone();
}

LocalTime::LocalTime(const std::string& timezone, const Time& t) :
        Time(t),
        mTimezone(timezone)
{
    SetTimezone();
}

LocalTime::LocalTime(const std::string& timezone, const time_t& t) :
    Time(t),
    mTimezone(timezone)
{
    SetTimezone();
}

LocalTime::LocalTime(const std::string& timezone, const timespec& t) :
    Time(t),
    mTimezone(timezone)
{
    SetTimezone();
}

LocalTime::LocalTime(const std::string& timezone, const struct tm& t) :
    Time(t),
    mTimezone(timezone)
{
    SetTimezone();
}

LocalTime::~LocalTime()
{
}

int
LocalTime::HourOfDay() const
{
    struct tm t;
    time_t l = mBigTime.mSeconds;
    localtime_r(&l, &t);

    return t.tm_hour;
}

std::string LocalTime::Print() const
{
    struct tm t;
    time_t l = mBigTime.mSeconds;
    localtime_r(&l, &t);

    char buffer[500];
    asctime_r(&t, buffer);
    std::string r(buffer);
    r = cor::Trim(r);
    return r;
}

void
LocalTime::SetTimezone()
{
    setenv("TZ", mTimezone.c_str(), 1);
    tzset();
}

// source: https://gist.github.com/diabloneo/9619917
double
operator-(struct timespec& x, struct timespec& y)
{
    struct timespec result;

    result.tv_sec  = x.tv_sec  - y.tv_sec;
    result.tv_nsec = x.tv_nsec - y.tv_nsec;
    if (result.tv_nsec < 0) {
        --result.tv_sec;
        result.tv_nsec += 1000000000L;
    }

    return result.tv_sec >= 0 ?
        result.tv_sec + result.tv_nsec / 1e9:
        result.tv_sec - result.tv_nsec / 1e9;
}
    
}
