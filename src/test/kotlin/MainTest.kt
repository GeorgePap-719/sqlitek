package io.sqlitek

import java.lang.StringBuilder
import kotlin.random.Random
import kotlin.test.Test


class MainTest {

    @Test
    fun basicTest() {
        val table = Table()
        val statements = createInsertStatements(100)
        for (statement in statements) {
            val result = createConnection(statement, table)
            assert(result is ConnectionResult.Success)
        }
        val select = selectStatement()
        createConnection(select, table)
    }

    // Our db can hold 1400 rows right now because we set the maximum number of pages to 100,
    // and 14 rows can fit in a page.
    @Test
    fun largeInsertTest() {
        val table = Table()
        val statements = createInsertStatements(1401)
        for (statement in statements) {
            val result = createConnection(statement, table)
            assert(result is ConnectionResult.Success)
        }
        val select = selectStatement()
        createConnection(select, table)
    }

    @Test
    fun allowMaxStrings() {
        val table = Table()
        val statement = insertStatement(
            1,
            buildString(32),
            buildString(255)
        )
        val result = createConnection(statement, table)
        assert(result is ConnectionResult.Success)
        val select = selectStatement()
        createConnection(select, table)
    }

    @Test
    fun doNotAllowGtMaxStrings() {
        val table = Table()
        val statement = insertStatement(
            1,
            buildString(33),
            buildString(256)
        )
        val result = createConnection(statement, table)
        check(result is ConnectionResult.Failure)
        println(result.message)
    }

    @Test
    fun doNotAllowNegativeId() {
        val table = Table()
        val statement = insertStatement(
            -1,
            buildString(33),
            buildString(256)
        )
        val result = createConnection(statement, table)
        check(result is ConnectionResult.Failure)
        println(result.message)
    }
}

// Utils

fun buildString(size: Int): String {
    val sb = StringBuilder(size)
    repeat(size) {
        sb.append("a")
    }
    return sb.toString()
}

fun closeConnectionCommand(): String {
    return ".exit"
}

fun createInsertStatements(until: Int): List<String> {
    val ids = 1..until
    return buildList {
        for (id in ids) {
            val username = "foo"
            val email = "bar@kmail.com"
            val statement = insertStatement(id, username, email)
            add(statement)
        }
    }
}

fun insertStatement(
    id: Int,
    username: String,
    email: String
): String {
    return "insert $id $username $email"
}

fun selectStatement(): String = "select"
