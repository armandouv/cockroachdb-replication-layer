//
// Created by armandouv on 30/12/22.
//

#ifndef CRDB_REPLICATION_LAYER_COMMAND_H
#define CRDB_REPLICATION_LAYER_COMMAND_H

enum OpType {
    CREATE,
    READ,
    UPDATE,
    DELETE
};

// A Command is a sequence of low-level changes to be applied to the underlying key-value store.
// For simplicity's sake, here we only consider a single change.
struct Command {
    OpType type;
    int key;
    int value;
};

#endif //CRDB_REPLICATION_LAYER_COMMAND_H
