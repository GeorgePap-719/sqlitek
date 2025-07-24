package github.io

import github.io.RowLayout.ROW_SIZE
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

// +const uint32_t PAGE_SIZE = 4096;
//+#define TABLE_MAX_PAGES 100
//+const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
//+const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
//+
//+typedef struct {
//+  uint32_t num_rows;
//+  void* pages[TABLE_MAX_PAGES];
//+} Table;

// I’m making our page size 4 kilobytes because it’s the same size as a page used in the virtual memory systems of most computer architectures.
// This means one page in our database corresponds to one page used by the operating system.
// The operating system will move pages in and out of memory as whole units instead of breaking them up.
const val PAGE_SIZE = 4096
const val TABLE_MAX_PAGES = 100

const val ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
const val TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES


// We don’t have void* or fixed-size arrays in Kotlin, so we represent it using:
//
//A mutable list or array of nullable ByteArrays or ByteBuffers
// Rows should not cross page boundaries.
// Since pages probably won’t exist next to each other in memory, this assumption makes it easier to read/write rows
class Table {
    var numberOfRows = 0

    val pages: Array<ByteArray?> = arrayOfNulls(TABLE_MAX_PAGES)
}

fun rowSlot(table: Table, rowNumber: Int): ByteBuffer {
    val pageNum = rowNumber / ROWS_PER_PAGE
    val rowOffset = rowNumber % ROWS_PER_PAGE
    val byteOffset = rowOffset * ROW_SIZE
    if (table.pages[pageNum] == null) {
        table.pages[pageNum] = ByteArray(PAGE_SIZE)
    }
    val page = table.pages[pageNum]!!
    return ByteBuffer.wrap(page, byteOffset, ROW_SIZE).slice()
}