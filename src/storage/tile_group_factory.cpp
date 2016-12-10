//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_factory.cpp
//
// Identification: src/storage/tile_group_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/tile_group_factory.h"
#include "logging/logging_util.h"
#include "storage/tile_group_header.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

// Logging mode
extern peloton::LoggingType peloton_logging_mode;

namespace peloton {
namespace storage {

TileGroup *TileGroupFactory::GetTileGroup(
    oid_t database_id, oid_t table_id, oid_t tile_group_id,
    AbstractTable *table, const std::vector<catalog::Schema> &schemas,
    const column_map_type &column_map, int tuple_count, int numa_region) {
  // Allocate the data on appropriate backend
  BackendType backend_type =
      logging::LoggingUtil::GetBackendType(peloton_logging_mode);

  // TODO We should also check numa_region to perform NUMA-unaware allocation
  TileGroupHeader *tile_header =
      new (numa_region) TileGroupHeader(backend_type, tuple_count);
  TileGroup *tile_group = new (numa_region) TileGroup(
      backend_type, tile_header, table, schemas, column_map, tuple_count, numa_region);

  tile_header->SetTileGroup(tile_group);

  tile_group->database_id = database_id;
  tile_group->tile_group_id = tile_group_id;
  tile_group->table_id = table_id;

  return tile_group;
}

}  // End storage namespace
}  // End peloton namespace
