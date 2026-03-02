//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_time__
#define __mcor_time__


#include <string>
#include <time.h>

#include <mcor/bigtime.h>
#include <mcor/mexception.h>


// TS7680 does not have C++11 compliant compiler yet
#if __cplusplus == 201103L
#define EXPLICIT explicit
#else
#define EXPLICIT
#endif


namespace cor
{


/** Converts time representations around
 *
 */
class Time
{
public:
    enum Initialize { eNow, eInvalid };
    Time(Initialize = eNow); // now
    Time(const time_t& t);
    Time(const timespec& t);
    Time(const BigTime& t);
    Time(int64_t seconds, int64_t attoSeconds);
    Time(const struct tm& t);
    // one-based index; Jan 1 2024 is (2024, 1, 1, 0.0)
    Time(int year, int month, int day, double second);
    // default format is same as that of Print(false) output
    Time(const std::string& t, const char* format = NULL);

    static Time PositiveInf();
    static Time NegativeInf();
    static Time Zero();

    // duration equal to one second
    static Time OneSecond();
    // duration equal to one minute
    static Time OneMinute();
    // duration equal to one hour
    static Time OneHour();
    // duration equal to one day
    static Time OneDay();
    // duration equal to one week
    static Time OneWeek();
    // duration equal to one 31-day month
    static Time OneMonth();
    // duration equal to one 365-day year
    static Time OneYear();


    static int HoursPerDay() { return 24; }
    static int DaysPerWeek() { return 7; }

    virtual ~Time();

    bool Valid() const;
    bool IsOrdinary() const;
    bool IsInfinite() const;
    bool IsPositiveInfinite() const;
    bool IsNegativeInfinite() const;
    bool IsZero() const;
    // XXX not sure negative ordinary times work correctly
    bool IsNegative() const;
    bool IsPositive() const;

    // see 'man strptime' for complete format options:
    //   %Y     The year, including century (for example, 1991)
    //   %m     The month number (1–12)
    //   %d     The day of month (1–31)
    //   %H     The hour (0–23)
    //   %M     The minute (0–59)
    //   %S     The second (0–60; 60 may occur for leap seconds; earlier also 61 was allowed)
    void FromString(const std::string& s, const char* format = NULL);
    // must be %H:%M:%S
    void TimeOfDayFromString(const std::string& s);

    std::string ToString(const char* format) const;
    std::string Date() const;
    std::string TimeOfDay() const;

    // this is rare; avoid confusing compiler with constructor
    static Time FromDouble(double d);

    double ToDouble() const;

    struct timespec ToTimeSpec() const { return mBigTime.ToTimeSpec(); }
    BigTime ToBigTime() const { return mBigTime; }

    Time& operator +=(const cor::Time& other);
    Time& operator +=(double seconds);
    Time& operator ++(int);
    Time& operator ++();
    Time& operator -=(const cor::Time& other);
    Time& operator -=(double seconds);
    Time& operator --(int);
    Time& operator --();
    Time& operator *=(const cor::Time& other);
    Time& operator *=(double seconds);
    Time& operator /=(const cor::Time& other);
    Time& operator /=(double seconds);

    struct tm ToTm() const;
    int HourOfDay() const;
    void SetHourOfDay(int hourOfDay);
    int Minute() const;
    void SetMinute(int minute);
    int Second() const;
    void SetSecond(int second);
    int SecondOfDay() const;
    void SetSecondOfDay(int second);
    int DayOfWeek() const; // 0 based
    void DayOfWeek(std::string& dow); // full capitalized word, 'Monday', etc.
    int Day() const; // day of month, 1 based
    void SetDay(int dayOfMonth); // 1 based
    int WeekOfMonth() const; // 1 based
    int DayOfMonth() const; // 1 based
    int Month() const; // 1 based
    void SetMonth(int month); // 1 based
    int Year() const;
    int DayOfYear() const; // 1 based
    double Fraction() const; // partial second

    // returns whether other is the same day as this
    bool SameDay(const cor::Time& other) const;

    // return time at top of [second, minute, hour, day]
    //  quantity = 0, direction = eBackwards :
    //       top of current minute (time moves back 60 seconds or less)
    //  quantity = 0, direction = eForwards :
    //        top of next minute (time moves forward 60 seconds or less)
    //  quantity = 1, direction = eBackwards :
    //        top of previous minute (time moves back 60 seconds or more)
    //   etc.
    enum eSnapDirection { eBackwards, eForwards };
    Time TopOfSecond(eSnapDirection direction, int quantity) const;
    Time TopOfMinute(eSnapDirection direction, int quantity) const;
    Time TopOfHour(eSnapDirection direction, int quantity) const;
    Time TopOfDay(eSnapDirection direction, int quantity) const;

    virtual std::string Print(bool decimal = false) const;
    void Print(std::ostream&, bool decimal = false) const;
    // produces 32 hex digits as a string representing time
    std::string HexDump() const;
    // set time based on hex digit string in same format as HexDump output
    void FromHexDump(const std::string& hex);

    // round to nearest second
    void Round();
    // round to last second
    void Floor();
    // round to next second
    void Ceiling();

    // round forward to next hour; does not advance if on the hour
    Time NextHour() const;
    // round to next hour of day
    Time NextHourOfDay(int hourOfDay) const;
    // round to next day; does not advance if midnight
    Time NextDay() const;
    // round to next day of week; does not advance if at midnight on the day of week
    Time NextDayOfWeek(int dayOfWeek) const;
    // round to next week
    Time NextWeek() const { return NextDayOfWeek(0); }
    // round to next day-of-month; if the month does not have this day, the
    // final day of the month is used.
    //    on Jan 30, NextDayOfMonth(30) = Feb 28/29
    //    on Feb 15, NextDayOfMonth(30) = Feb 28/29
    //    on Mar 30, NextDayOfMonth(30) = Apr 30
    Time NextDayOfMonth(int weekOfMonth) const;
    // round to next month
    Time NextMonth() const;
    // round to next month of year
    Time NextMonthOfYear(int monthOfYear) const;

    // scale
    void Scale(double factor);

    // return the midpoint between this and other
    Time Average(const Time& other) const;

    int64_t SecondPortion() const;
    //int64_t NanoSecondPortion() const;
    int64_t AttoSecondPortion() const;
    static int32_t AttoSecondToNanoSecond(int64_t asec);

    // ==== duration units

    // number of seconds
    int Seconds() const;
    // number of minutes
    int Minutes() const;
    // number of hours
    int Hours() const;
    // number of days
    int Days() const;
    // number of weeks
    int Weeks() const;
    // number of 30-day months
    int Months() const;
    // number of 365-day years
    int Years() const;

    // create string in duration units above, e.g.,
    //    '1 year, 7 months, 4 days, 2 hours, 43 minutes, and 1 second'
    // if unitMax is non-zero, will produce no more than that many
    // units with truncation (not rounding), e.g., if unitMax==2
    //    '1 year, 7 months'
    std::string DurationString(size_t unitMax = 0) const;

protected:
    enum State { eStateInvalid, eStateOrdinary, eStatePositiveInf, eStateNegativeInf};
    State mState;
    BigTime mBigTime;
};

Time operator +(const Time& t, double seconds);
Time operator +(double seconds, const Time& t);
Time operator +(const Time& t1, const Time& t2);
Time operator *(const Time& t, double d);
Time operator *(double d, const Time& t);
Time operator *(const Time& t, int n);
Time operator *(int n, const Time& t);
Time operator -(const Time& t, double seconds);
Time operator -(double seconds, const Time& t);
Time operator -(const Time& t1, const Time& t2);
bool operator >(const Time& t1, const Time& t2);
bool operator >=(const Time& t1, const Time& t2);
bool operator <(const Time& t1, const Time& t2);
bool operator <=(const Time& t1, const Time& t2);
bool operator ==(const Time& t1, const Time& t2);
bool operator !=(const Time& t1, const Time& t2);


class LocalTime : public Time
{
public:
    LocalTime(const std::string& timezone); // now
    LocalTime(const std::string& timezone, const cor::Time& t);
    LocalTime(const std::string& timezone, const time_t& t);
    LocalTime(const std::string& timezone, const timespec& t);
    LocalTime(const std::string& timezone, const struct tm& t);

    virtual ~LocalTime();

    int HourOfDay() const;

    std::string Print() const;
    void Print(std::ostream&) const;

protected:
    void SetTimezone();
    const std::string mTimezone;
};

std::ostream& operator<<(std::ostream& os, const Time& t);

// returns time, in seconds, between timespecs
double operator-(struct timespec& x, struct timespec& y);
    
}

#endif
