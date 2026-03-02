//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor__producer__
#define __mcor__producer__

#include <iostream>
#include <string>
#include <sstream>
#include <list>
#include <map>

#include <mcor/mexception.h>
#include <mcor/mmutex.h>

namespace cor {


/** template class Consumer
 */

template <class T>
class Consumer
{
public:
	Consumer();
    virtual ~Consumer();
    
    virtual void ProcessData(const T& data, int channel) = 0;

    mutable bool mConnected;
};
    
/** template class Producer
 */

template <class T>
class Producer
{
public:
	Producer();
    virtual ~Producer();

    virtual void PostData(const T& data);

    virtual void Connect(Consumer<T>& consumer, int channel);
    virtual void Disconnect(Consumer<T>& consumer);
    
protected:
    MMutex mMutex;
    typedef std::map<Consumer<T>*, int> ConsumerMap;
    ConsumerMap mConsumers;
};


/** template class Observable : version of Producer that
 *  tracks a state, rather than ephemeral data;
 *
 *  1) invokes ProcessData for current state upon Connect
 *  2) invokes ProcessData subsequently only when state changes
 *
 */

template <class T>
class Observable : public Producer<T>
{
public:
    Observable();
    virtual ~Observable();

    virtual void PostData(const T& data);

    virtual void Connect(Consumer<T>& consumer, int channel);

protected:
    mutable cor::MMutex mStateMutex;
    T mState;
};

/** template class ConsumerProducer
 *
 *  This class can be useful to:
 *     * adjust or filter the data between a Producer/Consumer
 *     * provide a proxy object if either Producer or Consumer are ephemeral
 *     * provide single-threaded behavior of the Consumer interface, because the
 *       producer MMutex is locked before PostData (so you won't consume from two
 *       threads in an interleaved way)
 */

template <class T>
class ConsumerProducer : public Consumer<T>, public Producer<T>
{
public:
    ConsumerProducer();
    virtual ~ConsumerProducer();

    virtual void ProcessData(const T& data, int channel);
};


/* ===================================================================
 * Consumer Implementation
 */

template <class T>
Consumer<T>::Consumer() : mConnected(false)
{}

template <class T>
Consumer<T>::~Consumer()
{
	if (mConnected)
	{
		std::cout << "WARNING: consumer connected at destruction" << std::endl;
	}
}


/* ===================================================================
 * Producer Implementation
 */

template <class T>
Producer<T>::Producer() : mMutex("Producer mutex")
{}

template <class T>
Producer<T>::~Producer()
{
	ObjectLocker ol(mMutex);
	typename ConsumerMap::const_iterator csci = mConsumers.begin();
	for (; csci != mConsumers.end(); csci++)
		csci->first->mConnected = false;
}

template <class T>
void
Producer<T>::PostData(const T& data)
{
	ObjectLocker ol(mMutex);
	typename ConsumerMap::const_iterator csci = mConsumers.begin();
	for (; csci != mConsumers.end(); csci++)
		csci->first->ProcessData(data, csci->second);
}

template <class T>
void
Producer<T>::Connect(Consumer<T>& consumer, int channel)
{
    ObjectLocker ol(mMutex);
	mConsumers[&consumer] = channel;
	consumer.mConnected = true;
}

template <class T>
void
Producer<T>::Disconnect(Consumer<T>& consumer)
{
	ObjectLocker ol(mMutex);
	typename ConsumerMap::iterator i = mConsumers.find(&consumer);
	if (i != mConsumers.end())
	{
		i->first->mConnected = false;
		mConsumers.erase(i);
	}
}


/* ===================================================================
 * Observable implementation
 */

template <class T>
Observable<T>::Observable() : mStateMutex("ObserverState")
{
    mState = T();
}

template <class T>
Observable<T>::~Observable()
{}

template <class T>
void
Observable<T>::PostData(const T& data)
{
    bool delta;
    {
        cor::ObjectLocker ol(mStateMutex);
        delta = mState != data;
    }

    if (delta)
    {
        //printf("Posting %d\n", delta);
        Producer<T>::PostData(data);
        {
            cor::ObjectLocker ol(mStateMutex);
            mState = data;
        }
    }
    //else
    //    printf("No delta\n");
}

template <class T>
void
Observable<T>::Connect(Consumer<T>& consumer, int channel)
{
    cor::ObjectLocker ol(mStateMutex);
    Producer<T>::Connect(consumer, channel);
    Producer<T>::PostData(mState);
}


/* ===================================================================
 * ConsumerProducer implementation
 */

template <typename T>
ConsumerProducer<T>::ConsumerProducer()
{
}

template <typename T>
ConsumerProducer<T>::~ConsumerProducer()
{
}

template <typename T>
void
ConsumerProducer<T>::ProcessData(const T& data, int channel)
{
    this->PostData(data);
}

}


#endif
