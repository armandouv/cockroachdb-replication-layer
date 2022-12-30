# Simulation of CockroachDB's distribution layer

The distribution layer is responsible for distributing data across the nodes in a cluster. It consists of a range-based
distributed key-value store, where each Range is a contiguous portion of the key space. Each Range is stored on at least
3 nodes in the cluster, and the distribution layer is responsible for managing the placement and movement of ranges
across nodes as the cluster grows or shrinks. In order to find where a certain range is placed, the distribution layer
uses a consistent hashing algorithm to map keys to ranges, and stores metadata about the ranges and their locations in a
distributed range descriptor table (here we hand a copy of the whole range descriptor table to each node). The
distribution layer also handles read and write requests from clients, forwarding them to the appropriate nodes and
returning the results to the clients. The distribution layer works in conjunction with the replication layer, which is
responsible for replicating data within a range for fault tolerance and ensuring data consistency across nodes. Since
the Distribution layer presents the abstraction of a single, monolithic key space, the SQL layer can perform read and
write operations for any Range on any node.
The workflow of this simulation is roughly as follows:

- We initialize the Distribution Layer, creating the specified number of nodes and creating a fixed number of fixed-size
  Ranges (this is for simplicity's sake, but in the real implementation ranges grow and split, or shrink and merge
  dynamically), assigning them to random nodes to serve as leaders, leaseholders, or normal replicas.
- We perform CRUD operations on the monolithic key-value store abstraction that the DistributionLayer presents. When
  we do this, the steps taken are roughly the following:
    1. The DistributionLayer class acts as a client, and can contact any node in the cluster to perform queries.
       To express this behavior, we first convert the specified operation to a Command (which is a series of low-level
       operations and serves as the minimum unit of replication), then choose a random node in the cluster and
       send the command.
    2. Once the command arrives at the node, it will search in a table of RangeDescriptors for the Range that is
       responsible for the key specified in the command.
    3. Having the appropriate RangeDescriptor, the node will check if it is the leaseholder for that Range. If so,
       it can start processing the command (move to step 4). Otherwise, it will forward it to the leaseholder
       (returning to step 2).
    4. Once the node knows it is the leaseholder of the range responsible for handling the key, it will propose the
       command to the leader (because it's the only node in the Range's Raft group allowed to do so).
    5. Once the command is proposed to the leader, it will start processing the command as follows:

        - If it's a READ operation, it will just return the local result it gets from performing the operation.
        - Else:
            - It will push the command to its own log, and make sure all other replicas do the same.
            - Once all replicas have pushed the command to their logs, the leader can commit the operation. Thus, the
              leader will finally apply the operation in its local key-value store, "send a commit message" to the
              remaining replicas (which will make them apply the command in their stores as well), and wait for them to
              finish.

    6. Now that the command is done processing, the leader returns the result to the leaseholder, the leaseholder in
       turn
       to the node in the cluster who made the request (if it was not initially the leaseholder), and finally to
       the client.

### Limitations

We made some assumptions that simplified the simulated process in comparison to the real implementation. Some
of them are:

- We don't implement expiration in Leases nor a Lease acquire mechanism, for which Raft is used.
- We don't have a distributed range descriptor table. Instead, we just pass a copy of the complete table to each node,
  using a std::map which is a balanced-search tree. This helps make fast lookups of ranges (O (log n)).
- The leaseholder and leader of a Range are determined manually here. In practice, this is done using the Raft
  algorithm, taking into account as well the distribution policies explained during the presentation.
- We use a fixed number of Ranges with a fixed size of keys. In the real implementation ranges grow and split, or
  shrink and merge dynamically.
- We obviously don't use network communication between nodes, which are represented by objects.
- We use a std::map to represent RocksDB.
- A Command only contains a single operation.
- We don't have a real Log, we use a queue to represent it.
- When simulating replication in the Raft algorithm, we check sequentially that each node completes the operation.
  Apart from this, instead of waiting for a majority of nodes to signal completion, we wait for all of them.
- Usually the system tries to assign the leaseholder and leader to be the same node, but for demonstration purposes
  we always have the leaseholder be a different node than the leader.

## Compilation and execution

#### Requirements

Working installations of CMake (version >= 3.22), make (or any other build system), and a C++ compiler (such as g++,
commonly included
while installing gcc) are needed.

#### Compiling

From the root directory:

`cmake CMakeLists.txt`

`make all`

#### Usage

`./distribution_layer`

Users can modify the main function in `distribution_layer.cpp` to perform different operations on the store.

#### Example output

A sample output can be found in `example.out`, which can be further analyzed for checking the simulation correctness.
