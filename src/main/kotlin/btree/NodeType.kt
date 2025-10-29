package io.sqlitek.btree

enum class NodeType(val value: Byte) {
    INTERNAL(0),
    LEAF(1);

    companion object {
        fun from(value: Byte): NodeType {
            for (type in entries) {
                if (value == type.value) return type
            }
            throw IllegalArgumentException("Invalid NodeType:$value")
        }
    }
}