//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_int_wrapper__
#define __mcor_int_wrapper__

/** template IntWrapper : wraps an integer as a class
 * 
 * @tparam T integer type
 */
// Source: http://coliru.stacked-crooked.com/view?id=f5566f15c11c52e2db7189d602cc601a-f674c1a6d04c632b71a62362c0ccfc51
template<class T>
class IntWrapper {
    T value;
public:
    typedef T value_type;
    IntWrapper() :value() {}
    IntWrapper(T v) :value(v) {}
    operator T() const {return value;}
    //modifiers
    IntWrapper& operator=(T v) {value=v; return *this;}
    IntWrapper& operator+=(T v) {value+=v; return *this;}
    IntWrapper& operator-=(T v) {value-=v; return *this;}
    IntWrapper& operator*=(T v) {value*=value; return *this;}
    IntWrapper& operator/=(T v) {value/=value; return *this;}
    IntWrapper& operator%=(T v) {value%=value; return *this;}
    IntWrapper& operator++() {++value; return *this;}
    IntWrapper& operator--() {--value; return *this;}
    IntWrapper operator++(int) {return IntWrapper(value++);}
    IntWrapper operator--(int) {return IntWrapper(value--);}
    IntWrapper& operator&=(T v) {value&=v; return *this;}
    IntWrapper& operator|=(T v) {value|=v; return *this;}
    IntWrapper& operator^=(T v) {value^=v; return *this;}
    IntWrapper& operator<<=(T v) {value<<=v; return *this;}
    IntWrapper& operator>>=(T v) {value>>=v; return *this;}
    //accessors
    IntWrapper operator+() const {return IntWrapper(+value);}
    IntWrapper operator-() const {return IntWrapper(-value);}
    IntWrapper operator!() const {return IntWrapper(!value);}
    IntWrapper operator~() const {return IntWrapper(~value);}
    //friends
    friend IntWrapper operator+(IntWrapper iw, IntWrapper v) {return iw+=v;}
    friend IntWrapper operator+(IntWrapper iw, T v) {return iw+=v;}
    friend IntWrapper operator+(T v, IntWrapper iw) {return IntWrapper(v)+=iw;}
    friend IntWrapper operator-(IntWrapper iw, IntWrapper v) {return iw-=v;}
    friend IntWrapper operator-(IntWrapper iw, T v) {return iw-=v;}
    friend IntWrapper operator-(T v, IntWrapper iw) {return IntWrapper(v)-=iw;}
    friend IntWrapper operator*(IntWrapper iw, IntWrapper v) {return iw*=v;}
    friend IntWrapper operator*(IntWrapper iw, T v) {return iw*=v;}
    friend IntWrapper operator*(T v, IntWrapper iw) {return IntWrapper(v)*=iw;}
    friend IntWrapper operator/(IntWrapper iw, IntWrapper v) {return iw/=v;}
    friend IntWrapper operator/(IntWrapper iw, T v) {return iw/=v;}
    friend IntWrapper operator/(T v, IntWrapper iw) {return IntWrapper(v)/=iw;}
    friend IntWrapper operator%(IntWrapper iw, IntWrapper v) {return iw%=v;}
    friend IntWrapper operator%(IntWrapper iw, T v) {return iw%=v;}
    friend IntWrapper operator%(T v, IntWrapper iw) {return IntWrapper(v)%=iw;}
    friend IntWrapper operator&(IntWrapper iw, IntWrapper v) {return iw&=v;}
    friend IntWrapper operator&(IntWrapper iw, T v) {return iw&=v;}
    friend IntWrapper operator&(T v, IntWrapper iw) {return IntWrapper(v)&=iw;}
    friend IntWrapper operator|(IntWrapper iw, IntWrapper v) {return iw|=v;}
    friend IntWrapper operator|(IntWrapper iw, T v) {return iw|=v;}
    friend IntWrapper operator|(T v, IntWrapper iw) {return IntWrapper(v)|=iw;}
    friend IntWrapper operator^(IntWrapper iw, IntWrapper v) {return iw^=v;}
    friend IntWrapper operator^(IntWrapper iw, T v) {return iw^=v;}
    friend IntWrapper operator^(T v, IntWrapper iw) {return IntWrapper(v)^=iw;}
    friend IntWrapper operator<<(IntWrapper iw, IntWrapper v) {return iw<<=v;}
    friend IntWrapper operator<<(IntWrapper iw, T v) {return iw<<=v;}
    friend IntWrapper operator<<(T v, IntWrapper iw) {return IntWrapper(v)<<=iw;}
    friend IntWrapper operator>>(IntWrapper iw, IntWrapper v) {return iw>>=v;}
    friend IntWrapper operator>>(IntWrapper iw, T v) {return iw>>=v;}
    friend IntWrapper operator>>(T v, IntWrapper iw) {return IntWrapper(v)>>=iw;}
};

#endif
