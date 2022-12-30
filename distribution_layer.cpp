//
// Created by armandouv on 22/05/22.
//

#include <bits/stdc++.h>
#include "node.h"

using namespace std;

int MAX_KEY = 100;


// We don't implement expiration in Leases nor lease acquires
class DistributionLayer {
    map<int, Node*> nodes_map_;
    int total_nodes_;

    [[nodiscard]] int get_random_node_id() const {
        return rand() % total_nodes_;
    }

    static void print_range_descriptor(const RangeDescriptor& descriptor) {
        cout << "RANGE DESCRIPTOR" << endl;
        cout << "id: " << descriptor.id << endl;
        cout << "start: " << descriptor.start << endl;
        cout << "end: " << descriptor.end << endl;
        cout << "leaseholder_id: " << descriptor.leaseholder_id << endl;
        cout << "leader_id: " << descriptor.leader_id << endl;
        cout << "replicas_id: { ";
        for (auto id : descriptor.replicas_id) cout << id << " ";
        cout << "}" << endl << endl << endl;
    }
public:
    // The number of nodes and replication factor must be >= 3.
    // Replication factor must be <= the number of nodes.
    DistributionLayer(int number_of_nodes, int replication_factor) : total_nodes_{number_of_nodes} {
        if (number_of_nodes < 3 || replication_factor < 3 || replication_factor > number_of_nodes)
            throw exception{};

        // Providing a seed value
        srand((unsigned) time(nullptr));

        // Since the distribution layer is in charge of knowing which node is the leaseholder for a particular Range, we
        // will maintain a sorted map (underlying balanced search tree) with the start value of the range as the key, and
        // the corresponding RangeDescriptor as value. This is so that we can find in O(log N) the Range to which the
        // searched key belongs. In order to do this, we can search for the largest value that is less than the key.
        // We will hand a copy of this map to every node, so that each one can find the appropriate Leaseholder.
        // In practice, this info is stored on System Ranges replicated in each node.
        map<int, RangeDescriptor> interval_start_to_range_descriptor;

        // First of all, we initialize Ranges
        // Originally, Ranges either:
        // - Grow and split, or
        // - Shrink and merge
        // This is done dynamically as the data inside each is added or deleted.
        // However, to simplify the simulation, here we only have a fixed number of Ranges = 2n, where n is the number
        // of nodes. We consider the keyspace to be the closed interval [0, MAX_KEY].
        // We divide the keyspace in n * 2 parts, so that we can have the same number of Ranges.
        int total_ranges = number_of_nodes * 2;
        int range_size = MAX_KEY / total_ranges;

        for (int i = 0; i < total_ranges; i++) {
            RangeDescriptor new_range;
            new_range.id = i;
            new_range.start = i * range_size;

            // If this is the last part, it may not have the same size as the other parts
            new_range.end = (i + 1) * range_size - 1;
            if (i == total_ranges - 1) {
                new_range.end = MAX_KEY - 1;
            }

            // The leaseholder and leader of a Range are determined manually here. In practice, this is done using the
            // Raft algorithm, taking into account as well the distribution policies explained during the presentation.

            // The leaseholder and the leader of a Range are often the same node, but they can be different. Here we let
            // them be different nodes to differentiate between their functions. We assign a random leader for each
            // Range. If the leader has id x, the leaseholder will be node x + 1, and remaining replicas will be
            // assigned to subsequent nodes (x + 2, x + 3...). We use % number_of_nodes to restart the assignment to
            // contiguous IDs.

            new_range.leader_id = rand() % number_of_nodes;
            new_range.leaseholder_id = (new_range.leader_id + 1) % number_of_nodes;

            new_range.replicas_id.insert(new_range.leader_id);
            new_range.replicas_id.insert( new_range.leaseholder_id);

            // Add remaining replicas
            int next_id = (new_range.leaseholder_id + 1) % number_of_nodes;
            for (int j = 0; j < replication_factor - 2; j++) {
                new_range.replicas_id.insert(next_id);
                next_id = (next_id + 1) % number_of_nodes;
            }

            interval_start_to_range_descriptor[new_range.start] = new_range;
            print_range_descriptor(new_range);
        }

        for (int i = 0; i < number_of_nodes; i++) {
            nodes_map_[i] = new Node{i, interval_start_to_range_descriptor};
        }
        // Once all nodes have been created, hand a copy of pointers to all of them
        for (const auto &[_, node] : nodes_map_) {
            node->AssignNodes(nodes_map_);
        }
    }

    ~DistributionLayer() {
        // Once all nodes have been created, hand a copy of pointers to all of them
        for (const auto &[_, node] : nodes_map_) {
            delete node;
        }
    }

    // The distribution layer is in charge of knowing which node is the leaseholder for a particular Range using a
    // consistent hashing scheme. However, here we act as a client and pick a random node to make the query. The queried
    // node then will have to find the appropriate Leaseholder.

    int Insert(int key, int value) {
        cout << "STARTING INSERTION OF PAIR (" + to_string(key) + ", " + to_string(value) + ")"<< endl;
        if (key < 0 || value < 0) {
            cout << "Key and value must be both nonnegative" << endl;
            cout << "INSERTION FAILED" << endl << endl << endl;
            return -1;
        }

        if (key > MAX_KEY){
            cout << "Key must be between 0 and MAX_KEY" << endl;
            cout << "INSERTION FAILED" << endl << endl << endl;
            return -1;
        }

        auto chosen_node = get_random_node_id();
        auto output = nodes_map_[chosen_node]->SendCommand({CREATE, key, value});
        if (output < 0) cout << "INSERTION FAILED" << endl << endl << endl;
        else cout << "INSERTION SUCCESSFUL" << endl << endl << endl;
        return output;
    }

    int Get(int key) {
        cout << "STARTING GET OF KEY " + to_string(key) << endl;
        if (key < 0) {
            cout << "Key and value must be both nonnegative" << endl;
            cout << "GET FAILED" << endl << endl << endl;
            return -1;
        }

        if (key > MAX_KEY){
            cout << "Key must be between 0 and MAX_KEY" << endl;
            cout << "GET FAILED" << endl << endl << endl;
            return -1;
        }

        auto chosen_node = get_random_node_id();
        auto output = nodes_map_[chosen_node]->SendCommand({READ, key});
        if (output < 0) cout << "GET FAILED" << endl << endl << endl;
        else cout << "GET SUCCESSFUL (VALUE = " + to_string(output) + ")" << endl << endl << endl;
        return output;
    }

    int Update(int key, int new_value) {
        cout << "STARTING UPDATE USING PAIR (" + to_string(key) + ", " + to_string(new_value) + ")"<< endl;
        if (key < 0 || new_value < 0) {
            cout << "Key and value must be both nonnegative" << endl;
            cout << "UPDATE FAILED" << endl << endl << endl;
            return -1;
        }

        if (key > MAX_KEY){
            cout << "Key must be between 0 and MAX_KEY" << endl;
            cout << "UPDATE FAILED" << endl << endl << endl;
            return -1;
        }

        auto chosen_node = get_random_node_id();
        auto output = nodes_map_[chosen_node]->SendCommand({UPDATE, key, new_value});
        if (output < 0) cout << "UPDATE FAILED" << endl << endl << endl;
        else cout << "UPDATE SUCCESSFUL" << endl << endl << endl;
        return output;
    }

    int Remove(int key) {
        cout << "STARTING DELETION OF KEY " + to_string(key) << endl;
        if (key < 0) {
            cout << "Key and value must be both nonnegative" << endl;
            cout << "DELETION FAILED" << endl << endl << endl;
            return -1;
        }

        if (key > MAX_KEY){
            cout << "Key must be between 0 and MAX_KEY" << endl;
            cout << "DELETION FAILED" << endl << endl << endl;
            return -1;
        }

        auto chosen_node = get_random_node_id();
        auto output = nodes_map_[chosen_node]->SendCommand({DELETE, key});
        if (output < 0) cout << "DELETION FAILED" << endl << endl << endl;
        else cout << "DELETION SUCCESSFUL" << endl << endl << endl;
        return output;
    }

    void PrintNodes() {
        for (const auto &[_, node] : nodes_map_) node->Print();
    }
};


int main() {
    DistributionLayer distribution_layer{5, 3};

    distribution_layer.Insert(1, 223);
    distribution_layer.Insert(10, 65422);
    distribution_layer.Insert(20, 2652);
    distribution_layer.Insert(30, 2542);
    distribution_layer.Insert(40, 652);
    distribution_layer.Insert(70, 265);
    distribution_layer.Insert(50, 298);
    distribution_layer.Insert(1000, 265);
    distribution_layer.Insert(-1, 298);
    distribution_layer.PrintNodes();

    distribution_layer.Get(1);
    distribution_layer.Get(10);
    distribution_layer.Get(20);
    distribution_layer.Get(30);
    distribution_layer.Get(40);
    distribution_layer.Get(31);
    distribution_layer.Get(41);
    distribution_layer.PrintNodes();

    distribution_layer.Update(1, 2223);
    distribution_layer.Update(10, 654224);
    distribution_layer.Update(20, 26352);
    distribution_layer.Update(30, 25842);
    distribution_layer.Update(40, 8652);
    distribution_layer.Update(32, 25842);
    distribution_layer.Update(49, 8652);
    distribution_layer.PrintNodes();

    distribution_layer.Remove(1);
    distribution_layer.Remove(10);
    distribution_layer.Remove(20);
    distribution_layer.Remove(30);
    distribution_layer.Remove(40);
    distribution_layer.Remove(31);
    distribution_layer.Remove(49);
    distribution_layer.PrintNodes();

    return 0;
}