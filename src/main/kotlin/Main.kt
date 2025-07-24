package github.io

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {
    val table = Table()
    while (true) {
        println("db > ")
        val input = readln()
        if (input.first() == '.') {
            val command = parseMetaCommand(input)
            if (command == null) {
                println("Unrecognized meta command:$input")
                continue
            }
            execute(command)
            continue
        }
        val statement = parsePrepareStatement(input)
        if (statement == null) {
            println("Unrecognized statement:$input")
            continue
        }
        execute(statement, table)
    }
}