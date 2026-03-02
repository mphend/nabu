//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_gate__
#define __mcor_gate__

#include <set>
#include <string>
#include <sstream>
#include "jsoncpp/json/json.h"

#include "mexception.h"
#include "mmutex.h"
#include "producer.h"

namespace cor
{


typedef std::set<std::string> Bits;


/** class Gate : a logic gate which takes action via callbacks when
 *  its inputs satisfy a logic condition specified by Algorithm
 *
 *  This is a "Strategy" pattern ala Gamma et. al.
 *
 * Threadsafe.
 */
template <class Algorithm>
class Gate
{
public:
    // construct with the names of the bits; these cannot be adjusted later
    Gate(const Bits& bits);
    virtual ~Gate();

    // override in derived classes to take action when the state of
    // the gate output changes; executed in the context of the thread
    // that triggers the change by calling Set()
    virtual void OnTrue() {}
    virtual void OnFalse() {}

    // set the named input to the given state
    void Set(const std::string& bit, bool state);
    // fetch the output state
    bool Get() const;

    void ToStream(std::ostream& s);
    void ToJson(Json::Value& v);

protected:
    mutable cor::MMutex mMutex;
    const Bits mBits;
    Bits mCurrentBits;
    bool mState; // cache of current state from last Process()

    Algorithm mAlgorithm;
};

// logical OR
class OrAlgorithm
{
public:
    bool Process(const Bits& currentBits, const Bits& allBits)
    {
        return !currentBits.empty();
    }
};

// logical AND
class AndAlgorithm
{
public:
    bool Process(const Bits& currentBits, const Bits& allBits)
    {
        return currentBits == allBits;
    }
};

typedef Gate<OrAlgorithm> OrGate;
typedef Gate<AndAlgorithm> AndGate;


/** class ObservableGate : implements Observable on a Gate
 *
 */
template <class Algorithm>
class ObservableGate : public Gate<Algorithm>, public Observable<bool>
{
public:
    // construct with the names of the bits; these cannot be adjusted later
    ObservableGate(const Bits& bits) : Gate<Algorithm>(bits) {}
    virtual ~ObservableGate() {}

protected:
    void OnFalse() { PostData(false); }
    void OnTrue() { PostData(true); }
};

typedef ObservableGate<OrAlgorithm> ObservableOrGate;
typedef ObservableGate<AndAlgorithm> ObservableAndGate;


/* ===================================================================
 * Gate Implementation
 */

template<class Algorithm>
Gate<Algorithm>::Gate(const Bits& bits) :
        mMutex("Gate"),
        mBits(bits),
        mState(false)
{
}

template< class Algorithm>
Gate<Algorithm>::~Gate()
{
}

template< class Algorithm>
void
Gate<Algorithm>::Set(const std::string& bit, bool state)
{
    cor::ObjectLocker ol(mMutex);
    if (mBits.find(bit) == mBits.end())
        throw cor::Exception("Illegal bit '%s' set in gate", bit.c_str());
    if (state)
        mCurrentBits.insert(bit);
    else
        mCurrentBits.erase(bit);

    bool newState = mAlgorithm.Process(mCurrentBits, mBits);
    if (newState && !mState)
    {
        mState = newState;
        OnTrue();
    }
    if (!newState && mState)
    {
        mState = newState;
        OnFalse();
    }
}

template< class Algorithm>
bool
Gate<Algorithm>::Get() const
{
    cor::ObjectLocker ol(mMutex);
    return mState;
}

template< class Algorithm>
void
Gate<Algorithm>::ToStream(std::ostream& os)
{
    cor::ObjectLocker ol(mMutex);
    os << (mState ? "True" : "False") << std::endl;
    std::set<std::string>::const_iterator csi = mBits.begin();
    for (; csi != mBits.end(); csi++)
    {
        os << "[" << *csi << "] ";
        os << ((mCurrentBits.find(*csi) != mCurrentBits.end()) ? "True" : "False");
        os << std::endl;
    }
}

template< class Algorithm>
void
Gate<Algorithm>::ToJson(Json::Value& v)
{
    cor::ObjectLocker ol(mMutex);
    v["state"] = mState;
    std::set<std::string>::const_iterator csi = mBits.begin();
    for (; csi != mBits.end(); csi++)
    {
        v["inputs"][*csi] = mCurrentBits.find(*csi) != mCurrentBits.end();
    }
}


}

#endif
