package io.sqlitek

import kotlin.test.Test


class MainTest {

    val dbFilename = "test.db"

    @Test
    fun basicTest() {
        val table = createConnection(dbFilename)
        val statements = createInsertStatements(10)
        for (statement in statements) {
            val result = parseInput(statement, table)
            assert(result is ConnectionResult.Success)
            println(result)
        }
        val select = selectStatement()
        val selectResult = parseInput(select, table)
        println(selectResult)
    }

    // Our db can hold 1400 rows right now because we set the maximum number of pages to 100,
    // and 14 rows can fit in a page.
    @Test
    fun largeInsertTest() {
        val table = createConnection(dbFilename)
        val statements = createInsertStatements(86)
        for (statement in statements) {
            val result = parseInput(statement, table)
            assert(result is ConnectionResult.Success)
        }
        val select = selectStatement()
        parsePrepareStatement(select)
        val print = printStatement()
        val meta = parseMetaCommand(print)!!
        execute(meta, table)
    }

    @Test
    fun testStr() {
        val sb = StringBuilder()
        sb.append("hello\n")
        sb.append("    ")
        sb.append("key")
        repeat(0 + 1) {
            println(sb.toString())
        }
    }


    @Test
    fun allowMaxStrings() {
        val table = createConnection(dbFilename)
        val statement = insertStatement(
            1,
            buildString(32),
            buildString(255)
        )
        val result = parseInput(statement, table)
        assert(result is ConnectionResult.Success)
        val select = selectStatement()
        parsePrepareStatement(select)
    }

    @Test
    fun doNotAllowGtMaxStrings() {
        val table = createConnection(dbFilename)
        val statement = insertStatement(
            1,
            buildString(33),
            buildString(256)
        )
        val result = parseInput(statement, table)
        check(result is ConnectionResult.Failure)
        println(result.message)
    }

    @Test
    fun doNotAllowNegativeId() {
        val table = createConnection(dbFilename)
        val statement = insertStatement(
            -1,
            buildString(33),
            buildString(256)
        )
        val result = parseInput(statement, table)
        check(result is ConnectionResult.Failure)
        println(result.message)
    }

    /**
     * Only this test simulates persistence,
     * because it is the only one we "close" properly the connection.
     */
    @Test
    fun testPersistence() {
        var table = createConnection(dbFilename)
        val statement = createInsertStatements(1).first()
        val result = parseInput(statement, table)
        assert(result is ConnectionResult.Success)
        // Simulate we are closing db:
        table.close()
        table = createConnection(dbFilename)
        val select = selectStatement()
        parseInput(select, table)
    }

    @Test
    fun testConstants() {
        // The tutorial tests against the constants, just for the sake of alerting.
        // We will skip it for now.
    }

    //TODO: passes, but we need to refactor the types for this to work.
    @Test
    fun testDuplicateId() {
        val table = createConnection(dbFilename)
        val statement = insertStatement(
            1,
            buildString(32),
            buildString(255)
        )
        val result = parseInput(statement, table)
        assert(result is ConnectionResult.Success)
        val result2 = parseInput(statement, table)
        println(result2)
        assert(result2 is ConnectionResult.Failure)
        println(result2)
    }

    @Test
    fun insert15Rows() {
        val table = createConnection(dbFilename)
        val insertStatements = createInsertStatements(15)
        for (statement in insertStatements) {
            val result = parseInput(statement, table)
            assert(result is ConnectionResult.Success)
        }
        val select = selectStatement()
        parseInput(select, table)
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

fun printStatement(): String = ".btree"
