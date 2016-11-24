//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile_factory.h
//
// Identification: src/include/executor/logical_tile_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <memory>

#include "common/types.h"

namespace peloton {

namespace storage {
class Tile;
class TileGroup;
class AbstractTable;
}

//===--------------------------------------------------------------------===//
// Logical Tile Factory
//===--------------------------------------------------------------------===//

namespace executor {
class LogicalTile;

class LogicalTileFactory {
 public:
  static LogicalTile *GetTile(size_t partition);

  static LogicalTile *WrapTiles(
      const std::vector<std::shared_ptr<storage::Tile>> &base_tile_refs,
      size_t partition);

  static LogicalTile *WrapTileGroup(
      const std::shared_ptr<storage::TileGroup> &tile_group, size_t partition);
};

}  // namespace executor
}  // namespace peloton
