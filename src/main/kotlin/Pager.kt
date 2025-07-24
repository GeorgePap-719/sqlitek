package io.sqlitek

import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile

// I’m making our page size 4 kilobytes because it’s the same size as a page used in the virtual memory systems of most computer architectures.
// This means one page in our database corresponds to one page used by the operating system.
// The operating system will move pages in and out of memory as whole units instead of breaking them up.
const val PAGE_SIZE = 4096

class Pager(
    val fileDescriptor: RandomAccessFile,
    // val pages: Array<ByteArray?> = arrayOfNulls(TABLE_MAX_PAGES)
    // Is this our cache?
    val cachedPages: Array<ByteArray?>
) : Closeable {
    val fileLength get() = fileDescriptor.length()

    // The get_page() method has the logic for handling a cache miss.
    // We assume pages are saved one after the other in the database file:
    // Page 0 at offset 0,
    // page 1 at offset 4096,
    // page 2 at offset 8192,
    // etc.
    // If the requested page lies outside the bounds of the file, we know it should be blank,
    // so we just allocate some memory and return it.
    // The page will be added to the file when we flush the cache to disk later.
    fun getPage(pageNumber: Int): ByteArray {
        require(pageNumber <= TABLE_MAX_PAGES) { "`pageNum` requests page out of bounds:$pageNumber" }
        val cachedPage = cachedPages[pageNumber]
        cachedPage?.let { return it }
        // Cache miss. Allocate memory and load from file.
        val page = ByteArray(PAGE_SIZE)
        var numberOfPages = fileLength / PAGE_SIZE
        // We might save a partial page at the end of the file.
        if (fileLength % PAGE_SIZE != 0L) numberOfPages++
        if (pageNumber <= numberOfPages) {
            // Move the cursor to the target offset.
            fileDescriptor.seek((pageNumber * PAGE_SIZE).toLong())
            fileDescriptor.read(page, 0, PAGE_SIZE)
        }
        cachedPages[pageNumber] = page
        return page
    }

    fun flush(pageNumber: Int, size: Int) {
        val page = cachedPages[pageNumber]
        checkNotNull(page) { "Tried to flush null page" }
        fileDescriptor.seek((pageNumber * PAGE_SIZE).toLong())
        fileDescriptor.write(page, 0, size)
    }

    override fun close() {
        fileDescriptor.close()
    }
}

fun openPager(filename: String): Pager {
    val file = File(filename)
    file.createNewFile() // checks also if it exists
    val descriptor = RandomAccessFile(file, "rw") // // Read/Write mode
    return Pager(descriptor, arrayOfNulls(TABLE_MAX_PAGES))
}