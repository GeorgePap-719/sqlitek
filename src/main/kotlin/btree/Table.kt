package io.sqlitek.btree

import io.sqlitek.RowLayout
import java.io.Closeable
import java.nio.ByteBuffer

const val TABLE_MAX_PAGES = 100

const val ROWS_PER_PAGE = PAGE_SIZE / RowLayout.ROW_SIZE
const val TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

// We don’t have void* in Kotlin, so we represent it using:
//
// A mutable list or array of nullable ByteArrays or ByteBuffers
// Rows should not cross page boundaries.
// Since pages probably won’t exist next to each other in memory, this assumption makes it easier to read/write rows
class Table(
    private val pager: Pager,
    val rootPageNumber: Int
) : Closeable {

    var numberOfPages: Int
        get() = pager.numberOfPages
        set(value) {
            pager.numberOfPages = value
        }

    fun getRootPage(): ByteBuffer = pager.getPage(rootPageNumber)
    fun getPage(number: Int): ByteBuffer = pager.getPage(number)
    fun getUnsuedPageNum(): Int = pager.getUnsuedPageNum()

    // - flushes the page cache to disk
    // - closes the database file
    // - frees the memory for the Pager and Table data structures
    override fun close() {
        val pager = pager
        val cachedPages = pager.cachedPages
        for (i in 0..<pager.numberOfPages) {
            if (cachedPages[i] == null) continue
            pager.flush(i)
            cachedPages[i] = null
        }
        pager.fileDescriptor.close()
        for (i in 0..<TABLE_MAX_PAGES) {
            if (pager.cachedPages[i] != null) pager.cachedPages[i] = null
        }
    }
}