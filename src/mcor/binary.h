//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_MCOR_BINARY_H
#define GICM_MCOR_BINARY_H

#include <stdint.h>

namespace cor
{


uint16_t Unpack16(const unsigned char* buffer);
uint32_t Unpack32(const unsigned char* buffer);
uint64_t Unpack64(const unsigned char* buffer);
double UnpackDouble(const unsigned char* buffer);

// advances the pointer by the size
uint16_t Unpack16(const unsigned char** buffer);
uint32_t Unpack32(const unsigned char** buffer);
uint64_t Unpack64(const unsigned char** buffer);
double UnpackDouble(const unsigned char** buffer);

void Pack16(unsigned char* buffer, uint16_t n);
void Pack32(unsigned char* buffer, uint32_t n);
void Pack64(unsigned char* buffer, uint64_t n);
void PackDouble(unsigned char* buffer, double n);

// advances the pointer by the size
void Pack16(unsigned char** buffer, uint16_t n);
void Pack32(unsigned char** buffer, uint32_t n);
void Pack64(unsigned char** buffer, uint64_t n);
void PackDouble(unsigned char** buffer, double n);
}
#endif

