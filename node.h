//
// Created by armandouv on 30/12/22.
//

#include <bits/stdc++.h>
#include "command.h"

using namespace std;

#ifndef CRDB_REPLICATION_LAYER_NODE_H
#define CRDB_REPLICATION_LAYER_NODE_H

struct RangeDescriptor {
    int id;
    int start;
    int end;
    int leader_id;
    int leaseholder_id;
    std::set<int> replicas_id;
};

class Node {
    int id_;
    map<int, RangeDescriptor> interval_start_to_range_descriptor_;
    // Ordered underlying key-value store (simulating RocksDB)
    map<int, int> key_value_store_;
    map<int, Node*> nodes_;
    vector<Command> log_;


    int ApplyCreate(int key, int value) {
        if (key_value_store_.contains(key)) {
            cout << "Key " + to_string(key) + " already exists in this node" << endl;
            return -1;
        }
        key_value_store_[key] = value;

        return 0;
    }

    int ApplyRead(int key) {
        if (!key_value_store_.contains(key)) {
            cout << "Key " + to_string(key) + " does not exist in this node" << endl;
            return -1;
        }
        return key_value_store_[key];
    }

    int ApplyUpdate(int key, int new_value) {
        if (!key_value_store_.contains(key)) {
            cout << "Key " + to_string(key) + " does not exist in this node" << endl;
            return -1;
        }

        key_value_store_[key] = new_value;
        return 0;
    }

    int ApplyDelete(int key) {
        if (!key_value_store_.contains(key)) {
            cout << "Key " + to_string(key) + " does not exist in this node" << endl;
            return -1;
        }
        key_value_store_.erase(key);
        return 0;
    }

    int ApplyCommand(const Command &command, const RangeDescriptor &range_descriptor) {
        // If it's a read we can just apply the command straight up.
        if (command.type == READ) return ApplyRead(command.key);

        // Normally, we would need to check if the command to apply is in the log, and if it is, remove it
        // In this program this will always be true, because we just added it at the end of the vector, and it's the one
        // we're going to apply. This can be generalized potentially using IDs in Commands, and an ordered data structure
        // so that an order can be kept in the log.

        // We assume command is the same as the one in the log, but we still that the log is not empty, because we can
        // have other that were never committed.
        if (log_.empty()) return -1;

        // We also check again if this node is responsible for the specified operation.
        if (!range_descriptor.replicas_id.contains(id_)) {
            cout << "The specified range is not in this node" << endl;
            return -1;
        }

        if (!(command.key >= range_descriptor.start && command.key <= range_descriptor.end)) {
            cout << "The specified key is not inside the range" << endl;
            return -1;
        }

        // This does not guarantee it's indeed the same command, we need to assign an id to each one
        auto last_log_entry = log_[log_.size() - 1];

        if (command.type != last_log_entry.type || command.key != last_log_entry.key
            || command.value != last_log_entry.value) {
            cout << "Command is not in log" << endl;
            return -1;
        }

        log_.erase(log_.end() - 1);

        switch (command.type) {
            case CREATE:
                return ApplyCreate(command.key, command.value);
            case UPDATE:
                return ApplyUpdate(command.key, command.value);
            case DELETE:
                return ApplyDelete(command.key);
            default:
                return -1;
        }
    }

    void PushCommandToLog(const Command &command) {
        log_.push_back(command);
        cout << "Command just pushed to Log of Node " + to_string(id_) << endl;
    }

    // This only executes in the leader
    int ProcessCommand(const Command &command, const RangeDescriptor &range_descriptor) {
        // check if this node is the leader of the specified range
        if (range_descriptor.leader_id != id_) {
            cout << "A node that is not the leader for a range cannot process a command" << endl;
            return -1;
        }

        // If it's a read, we can just return whatever the leader returns;
        if (command.type == READ) return ApplyCommand(command, range_descriptor);

        // This is where most of the replication layer logic is.

        // Replicate command to other nodes in the Range's Raft group, and wait until all have finished. In the real
        // implementation, we would only wait for the majority of nodes to replicate the command.
        PushCommandToLog(command);
        for (auto replica_id : range_descriptor.replicas_id) {
            if (replica_id == id_) continue; // We've already added to the leader's log
            nodes_[replica_id]->PushCommandToLog(command);
        }

        // Once all replicas have replicated the command, we are ready to commit. Thus, we sent a commit message to all
        // of them so that the command is actually applied in the key-value store. Here, the commit message is simulated
        // by the ApplyCommand method, which "receives" the commit message and applies the actual changes.

        int result = ApplyCommand(command, range_descriptor);
        // If we detect an error, we return it instead of continuing doing work. As a consequence, logs can have
        // uncommitted commands.
        if (result < 0) return result;

        for (auto replica_id : range_descriptor.replicas_id) {
            if (replica_id == id_) continue; // We've already applied the command in the leader
            result = nodes_[replica_id]->ApplyCommand(command, range_descriptor);
            if (result < 0) return result;
        }

        return result;
    }

    // This only executes in the leaseholder
    int SendCommandToLeader(const Command &command, const RangeDescriptor &range_descriptor) {
        // check if this node is the leaseholder of the specified range
        if (range_descriptor.leaseholder_id != id_) {
            cout << "A node that is not the leaseholder for a range cannot send commands to the leader" << endl;
            return -1;
        }

        return nodes_[range_descriptor.leader_id]->ProcessCommand(command, range_descriptor);
    }

public:
    Node(int id, const map<int, RangeDescriptor> &interval_start_to_range_descriptor)
            : id_{id}, interval_start_to_range_descriptor_{interval_start_to_range_descriptor} {
    }

    void AssignNodes(const map<int, Node*> &nodes) {
        nodes_ = nodes;
    }

    int SendCommand(const Command &command) {
        // Check it the key is inside a range that belongs to this Node
        if (interval_start_to_range_descriptor_.empty()) {
            cout << "Lookup table for ranges is empty" << endl;
            return -1;
        }

        auto it = interval_start_to_range_descriptor_.upper_bound(command.key);
        if (it == interval_start_to_range_descriptor_.begin()) {
            cout << "No range assigned for this value" << endl;
            return -1;
        }
        it--;

        auto range_descriptor = it->second;

        // This is the leaseholder for the appropriate Range
        if (range_descriptor.leaseholder_id == id_) return SendCommandToLeader(command, range_descriptor);

        // Forward the Command to the leaseholder
        return nodes_[range_descriptor.leaseholder_id]->SendCommand(command);
    }

    void Print() {
        cout << "Node with ID = " + to_string(id_) << endl;
        cout << "Log: [ ";
        for (const auto &command : log_) {
            cout << "{ type: " << command.type << ", key: "
                 << command.key << ", value: " << command.value << " }, ";
        }
        cout << "]" << endl;
        cout << "Key-Value store: [ ";
        for (const auto &[key, value] : key_value_store_) {
            cout << "{ " << key << ", " << value << " }, ";
        }
        cout << "]" << endl << endl << endl;
    }
};


#endif //CRDB_REPLICATION_LAYER_NODE_H