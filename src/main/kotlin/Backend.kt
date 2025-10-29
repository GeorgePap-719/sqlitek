package io.sqlitek

import io.sqlitek.RowLayout.ROW_SIZE
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