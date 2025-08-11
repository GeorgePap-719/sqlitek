package io.sqlitek

import io.sqlitek.RowLayout.ROW_SIZE
import java.io.Closeable
import java.nio.ByteBuffer

object RowLayout {
    // total 291
    const val ID_SIZE = Int.SIZE_BYTES               // 4 bytes
    const val USERNAME_SIZE = 32                     // fixed-length string (ASCII/UTF-8)
    const val EMAIL_SIZE = 255                       // fixed-length string

    const val ID_OFFSET = 0
    const val USERNAME_OFFSET = ID_OFFSET + ID_SIZE
    const val EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE

    const val ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

    val CHARSET = Charsets.UTF_8
}

fun serialize(row: Row): ByteArray {
    val buffer = ByteBuffer.allocate(ROW_SIZE)
    buffer.putInt(row.id)
    val usernameBytes = row.username.toByteArray().copyOf(RowLayout.USERNAME_SIZE)
    val emailBytes = row.email.toByteArray().copyOf(RowLayout.EMAIL_SIZE)
    buffer.put(usernameBytes)
    buffer.put(emailBytes)
    return buffer.array()
}

fun deserialize(input: ByteArray): Row {
    require(input.size == ROW_SIZE) { "Invalid row byte size:${input.size}" }
    val buffer = ByteBuffer.wrap(input)
    val id = buffer.int
    val usernameBytes = ByteArray(RowLayout.USERNAME_SIZE)
    buffer.get(usernameBytes)
    val username = usernameBytes.toString(RowLayout.CHARSET).trimEnd('\u0000')
    val emailBytes = ByteArray(RowLayout.EMAIL_SIZE)
    buffer.get(emailBytes)
    val email = emailBytes.toString(RowLayout.CHARSET).trimEnd('\u0000')
    return Row(id, username, email)
}


const val TABLE_MAX_PAGES = 100

const val ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
const val TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

// We don’t have void* in Kotlin, so we represent it using:
//
// A mutable list or array of nullable ByteArrays or ByteBuffers
// Rows should not cross page boundaries.
// Since pages probably won’t exist next to each other in memory, this assumption makes it easier to read/write rows
class Table(
    val pager: Pager,
    var rootPageNumber: Int
) : Closeable {

    fun getCursorValue(cursor: Cursor): ByteBuffer {
        val pageNum = cursor.pageNumber
        val page = cursor.pager.getPage(pageNum)
        return getLeafNodeValue(page, cursor.cellNumber)
    }

    fun cursorAdvance(cursor: Cursor) {
        val pageNumber = cursor.pageNumber
        val node = pager.getPage(pageNumber)
        cursor.cellNumber += 1
        val numCells = getLeafNodeNumCells(node)
        if (cursor.cellNumber >= numCells) cursor.endOfTable = true
    }

    override fun close() {
        closeDatabase(this)
    }
}

// flushes the page cache to disk
// closes the database file
// frees the memory for the Pager and Table data structures
fun closeDatabase(table: Table) {
    val pager = table.pager
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