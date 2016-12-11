//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value.cpp
//
// Identification: src/backend/common/numa_allocator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/numa_allocator.h"

namespace peloton {
namespace common{
NumaAllocator* NumaAllocator::instance = nullptr;
}
}
