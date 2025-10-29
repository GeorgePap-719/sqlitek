package io.sqlitek

import io.sqlitek.btree.LEAF_NODE_CELL_SIZE
import java.nio.ByteBuffer

/**
 * Similar to C's "memcpy".
 */
fun ByteBuffer.copyInto(
    dest: ByteBuffer,
    destOffset: Int = 0,
    srcOffset: Int = 0,
    length: Int = this.limit()
) {
    val src = this
    require(srcOffset >= 0 && srcOffset + length <= src.limit()) {
        "Source range out of bounds for length:$length"
    }
    require(destOffset >= 0 && destOffset + length <= dest.limit()) {
        "Destination range out of bounds for length:$length"
    }
    // Save old positions.
    val oldSrcPos = src.position()
    val oldDestPos = dest.position()
    // Copy directly.
    src.limit(srcOffset + length)
    src.position(srcOffset)
    dest.position(destOffset)
    dest.put(src)
    // Restore positions & limits.
    src.limit(src.capacity())
    src.position(oldSrcPos)
    dest.position(oldDestPos)
}

/**
 * Similar to C's "memmove".
 */
fun ByteBuffer.moveInto(
    dest: ByteBuffer,
    destOffset: Int = 0,
    srcOffset: Int = 0,
    length: Int = this.limit()
) {
    val src = this
    //TODO: we can improve this by avoiding the extra buffer.
    // We could just copy backwards.
    if (src == dest) {
        val buffer = ByteArray(LEAF_NODE_CELL_SIZE)
        src.position(srcOffset)
        src.get(buffer)
        dest.position(destOffset)
        dest.put(buffer)
        return
    }
    copyInto(dest, destOffset, srcOffset, length)
}