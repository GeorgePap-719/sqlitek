package io.sqlitek

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