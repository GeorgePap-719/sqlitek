package io.sqlitek

import io.sqlitek.RowLayout.ROW_SIZE
import io.sqlitek.btree.COMMON_NODE_HEADER_SIZE
import io.sqlitek.btree.LEAF_NODE_CELL_SIZE
import io.sqlitek.btree.LEAF_NODE_HEADER_SIZE
import io.sqlitek.btree.LEAF_NODE_MAX_CELLS
import io.sqlitek.btree.LEAF_NODE_SPACE_FOR_CELLS

val CONSTANTS = """
    ROW_SIZE: $ROW_SIZE
    COMMON_NODE_HEADER_SIZE: ${COMMON_NODE_HEADER_SIZE}
    LEAF_NODE_HEADER_SIZE: ${LEAF_NODE_HEADER_SIZE}
    LEAF_NODE_CELL_SIZE: ${LEAF_NODE_CELL_SIZE}
    LEAF_NODE_SPACE_FOR_CELLS: ${LEAF_NODE_SPACE_FOR_CELLS}
    LEAF_NODE_MAX_CELLS: ${LEAF_NODE_MAX_CELLS}
""".trimIndent()