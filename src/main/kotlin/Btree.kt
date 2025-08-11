package io.sqlitek

import io.sqlitek.RowLayout.ROW_SIZE
import java.nio.ByteBuffer

/*
 * Common Node Header Layout
 */

const val NODE_TYPE_SIZE = UByte.SIZE_BYTES         // 1
const val NODE_TYPE_OFFSET = 0

const val IS_ROOT_SIZE = UByte.SIZE_BYTES           // 1
const val IS_ROOT_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE

const val PARENT_POINTER_SIZE = UInt.SIZE_BYTES     // 4
const val PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE

const val COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

/*
 * Leaf Node Header Layout
 */

const val LEAF_NODE_NUM_CELLS_SIZE = UInt.SIZE_BYTES            // 4
const val LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE

const val LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

/*
 * Leaf Node Body Layout
 */

const val LEAF_NODE_KEY_SIZE = Int.SIZE_BYTES                // 4
const val LEAF_NODE_KEY_OFFSET = 0

const val LEAF_NODE_VALUE_SIZE = ROW_SIZE
const val LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE

const val LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE

const val LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE

const val LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE

fun getLeafNodeNumCells(node: ByteBuffer): Int {
    // Read Int at the offset where num_cells is stored.
    return node.getInt(LEAF_NODE_NUM_CELLS_OFFSET)
}

fun setLeafNodeNumCells(node: ByteBuffer, value: Int) {
    node.putInt(LEAF_NODE_NUM_CELLS_OFFSET, value)
}

fun leafNodeCellOffset(cellNumber: Int): Int {
    return LEAF_NODE_HEADER_SIZE + cellNumber * LEAF_NODE_CELL_SIZE
}

fun getLeafNodeKey(node: ByteBuffer, cellNumber: Int): Int {
    val offset = leafNodeCellOffset(cellNumber)
    return node.getInt(offset) // read 4-byte key
}

fun setLeafNodeKey(node: ByteBuffer, cellNumber: Int, key: Int) {
    val offset = leafNodeCellOffset(cellNumber)
    node.putInt(offset, key)
}

fun leafNodeValueOffset(cellNum: Int): Int {
    return leafNodeCellOffset(cellNum) + LEAF_NODE_KEY_SIZE
}

fun getLeafNodeValue(node: ByteBuffer, cellNumber: Int): ByteBuffer {
    val offset = leafNodeValueOffset(cellNumber)
    val value = ByteArray(LEAF_NODE_VALUE_SIZE)
    node.position(offset)
    node.get(value)
    return ByteBuffer.wrap(value)
}

fun setLeafNodeValue(node: ByteBuffer, cellNum: Int, value: ByteArray) {
    require(value.size == LEAF_NODE_VALUE_SIZE) {
        "Value must be exactly $LEAF_NODE_VALUE_SIZE bytes"
    }
    val offset = leafNodeValueOffset(cellNum)
    node.position(offset)
    node.put(value)
}

fun initializeLeafNode(node: ByteBuffer) {
    setNodeType(node, NodeType.LEAF)
    setLeafNodeNumCells(node, 0)
}

fun getNodeType(node: ByteBuffer): NodeType {
    val value = node.get(NODE_TYPE_OFFSET)
    return NodeType.from(value)
}

fun setNodeType(node: ByteBuffer, type: NodeType) {
    node.put(NODE_TYPE_OFFSET, type.value)
}

fun leafNodeToStringDebug(node: ByteBuffer): String {
    return buildString {
        val num = getLeafNodeNumCells(node)
        append("leaf (size:$num)\n")
        for (i in 0..<num) {
            val key = getLeafNodeKey(node, i)
            append(" - $i : $key \n")
        }
    }
}

fun printLeafNode(node: ByteBuffer) {
    println(leafNodeToStringDebug(node))
}


// Each node will correspond to one page.
// Internal nodes will point to their children by storing the page number that stores the child.
// The btree asks the pager for a particular page number and gets back a pointer into the page cache.
// Pages are stored in the database file one after the other in order of page number.
//
// Nodes need to store some metadata in a header at the beginning of the page.
// Every node will store what type of node it is, whether or not it is the root node,
// and a pointer to its parent (to allow finding a node’s siblings).
// I define constants for the size and offset of every header field:
class Btree

enum class NodeType(val value: Byte) {
    INTERNAL(0),
    LEAF(1);

    companion object {
        fun from(value: Byte): NodeType {
            for (type in entries) {
                if (value == type.value) return type
            }
            throw IllegalArgumentException("Invalid NodeType:$value")
        }
    }
}


fun leafNodeInsert(cursor: Cursor, key: Int, value: Row) {
    val node = cursor.pager.getPage(cursor.pageNumber)
    val numCells = getLeafNodeNumCells(node)
    if (numCells >= LEAF_NODE_MAX_CELLS) {
        // Node is full.
        error("Need to implement splitting a leaf node.")
    }
    val cellNumber = cursor.cellNumber
    if (cellNumber < numCells) {
        // Shift cells to make room.
        for (i in numCells downTo cellNumber + 1) {
            val destOffset = leafNodeCellOffset(i)
            val srcOffset = leafNodeCellOffset(i - 1)
            val temp = ByteArray(LEAF_NODE_CELL_SIZE)
            node.position(srcOffset)
            node.get(temp)
            node.position(destOffset)
            node.put(temp)
        }
    }
    setLeafNodeNumCells(node, numCells + 1)
    setLeafNodeKey(node, cellNumber, key)
    val serialized = serialize(value)
    setLeafNodeValue(node, cellNumber, serialized)
}