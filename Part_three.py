"""
This method emulates the way one would deal with data sets that are too big to fit into memory by operating with files and iterative passes, based on distributed graph processing patterns.

1. Phase 1: Initial Setup and Adjacency Collection (Simulated)
    * Reading is done only once through input file.
    * cardholder_neighbors_tmp and merchant_neighbors_tmp: These defaultdict(set) are like a temporary storage space where the full adjacency lists are held. In an extremely huge scenario, these dictionaries would be too large to fit into your memory. So, you would store these adjacencies in multiple, smaller files on your disk (for instance cardholder_adj_part1.txt, merchant_adj_part1.txt, etc.). For demonstration only in Python, I have set them in the memory for simplicity purposes so that the new_cluster_votes function can operate. If these also exceed the capacity, then the main new_cluster_votes loop should get the neighbor information off of the disk for every entity that it processes, which is much slower.
    * entity_cluster_id_0.csv: This file is the place where the initial "cluster ID" of each entity is recorded. Here each entity's ID is the same as its name (for example, C1,C1). This is like an initial allocation in a distributed system.

2. Phase 2: Iterative Label Propagation (Simulated)
    * This is the main part of the "clustering" process when loading the entire graph is not an option.
    * num_iterations: The algorithm performs a fixed number of iterations. During every iteration, the entities change their cluster assignment, basing on their neighbors' current cluster assignments.
    * current_entity_clusters: In every iteration, updating from entity_id to the corresponding current cluster_id is done by reading the CSV file. Very important, if you work with a really huge dataset, this dictionary may also not be able to fit in the memory. Hence, you have to have a key-value store that reads and writes data from and to the disk (for example sqlite3, or the solution that you created on your own). Alternatively, you could resort to a distributed in-memory key-value store. The example I give assumes that storing this mapping is possible even though the entire adjacency graph cannot.
    * new_cluster_votes: For each entity, it gathers votes from its connected neighbors (using the cardholder_neighbors_tmp and merchant_neighbors_tmp, which again, would be read from disk in a real out-of-memory scenario). It also gives itself a vote to maintain stability.
    * next_cluster_id_file: The updated cluster assignments are written to a new file. This allows the process to iterate without holding all cluster state in memory.
    * Label Propagation Logic: Each entity adopts the cluster ID that has the most connections to it (among its neighbors and its own current cluster). This is a simple but effective heuristic for finding communities.

3. Phase 3: Final Cluster Aggregation and Duplication
    * After the num_iterations, the final_entity_clusters mapping (from entity_cluster_id_X.csv) is loaded.
    * Core Assignment: Entities are initially grouped into aggregated_clusters based on their final assigned cluster ID.
    * Duplication Logic: This is similar to Part 2's duplication phase. It re-iterates through each entity and its connections (again, cardholder_neighbors_tmp and merchant_neighbors_tmp are used here, which would ideally be read from sharded files). It counts interactions with each of the final aggregated clusters. If an entity has significant connections to a secondary cluster (based on duplication_threshold_ratio), it's added to that cluster as well.
    * Output: The final clusters (with duplicated entities) are written to a structured text file.
    * Cleanup: Temporary files are removed.

4. Refinements for True Out-of-Memory Scenarios:
    * Disk-Backed Data Structures: For cardholder_neighbors_tmp, merchant_neighbors_tmp, and current_entity_clusters to truly scale, you'd need libraries like sqlite3 (as a simple local database), shelve, or more robust solutions like Apache Parquet/HDF5 files combined with a custom lookup mechanism.
    * True Batching of Adjacency List Creation: Phase 1 actually should be done in a way, that it processes small batches of the input file, aggregates the current batch's adjacencies, and then merges the previous batches' adjacencies that are written to disk. This is a typical pattern utilized for "shuffling" data for distributed processing.
    * MapReduce/Spark Context: The natural flow of Phase 2 (Iterative Label Propagation) is a direct analogy of a MapReduce or Spark iterative job. During every iteration a pass over the graph happens, where "map" sends neighbor proposals and the "reduce" gathers votes to find out the new cluster ID.
    * Streaming Input/Output: Besides not reading full CSVs, the data can also be processed line by line and written line by line for a minimal memory footprint.
    
"""

# The address for the main part 3 here is to illustrate the outlines of out-of-memory processing (iterative passes, external storage for state, batching), but with a Python code that is still executable without the need of external distributed frameworks. For extremely large datasets, a software platform based on Apache Spark is highly suggestible. 
""" 
This program will generate the following files and directories in its root folder:
- large_transactions.txt => A large test dataset simulating transactions between cardholders and merchants.
- large_cluster_output/
- large_cluster_output/entity_cluster_id_0.csv
"""
import collections  # For collections like defaultdict and Counter
import os   # For file operations
import csv # For structured output
import random  # For generating test data

def process_large_dataset_batch(input_filepath, output_directory, num_iterations=5, duplication_threshold_ratio=0.3):
    """
    Processes large datasets by reading in batches and writing intermediate results to files,
    simulating out-of-memory processing for clustering with duplication.

    Args:
        input_filepath (str): Path to the input transaction file.
        output_directory (str): Directory to write intermediate and final output files.
        num_iterations (int): Number of label propagation iterations.
        duplication_threshold_ratio (float): Threshold for duplicating entities.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # --- Phase 1: Create initial entity-to-ID mapping and adjacency counts (Batch Read) ---
    # We can't build full sets, so we'll aggregate counts in batches
    # and write to temporary files.
    
    # Store unique entities and give them an initial cluster ID (their own ID)
    # This simulates distributed state.
    entity_to_id_file = os.path.join(output_directory, "entity_cluster_id_0.csv")
    
    # These will be aggregators that are emptied after each pass
    cardholder_neighbors_tmp = collections.defaultdict(set)
    merchant_neighbors_tmp = collections.defaultdict(set)

    all_unique_entities = set() # To ensure all are handled in initial mapping

    with open(input_filepath, 'r') as infile:
        for line_num, line in enumerate(infile):
            c, m = line.strip().split()
            all_unique_entities.add(c)
            all_unique_entities.add(m)
            # In a true distributed system, this would be sharded
            # For simulation, just keep track of all interactions
            cardholder_neighbors_tmp[c].add(m)
            merchant_neighbors_tmp[m].add(c)
            
    # Write initial cluster IDs (each entity is its own cluster)
    with open(entity_to_id_file, 'w', newline='') as f:
        writer = csv.writer(f)
        for entity in all_unique_entities:
            writer.writerow([entity, entity]) # Entity ID is its initial cluster ID

    print(f"Phase 1: Initial entity cluster IDs created at {entity_to_id_file}")

    # --- Phase 2: Iterative Label Propagation (Simulated) ---
    # In each iteration, entities propose cluster IDs to their neighbors.
    # Each entity then adopts the most frequent ID among its neighbors and its own.

    current_cluster_id_file = entity_to_id_file

    for iteration in range(num_iterations):
        print(f"Starting iteration {iteration + 1}/{num_iterations}...")
        next_cluster_id_file = os.path.join(output_directory, f"entity_cluster_id_{iteration + 1}.csv")
        
        # Read current cluster IDs into memory (assuming it fits, if not, need a disk-backed dict)
        # For truly huge scale, this mapping itself would be distributed.
        current_entity_clusters = {}
        with open(current_cluster_id_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                current_entity_clusters[row[0]] = row[1]
        
        # Collect votes for new cluster assignments
        new_cluster_votes = collections.defaultdict(collections.Counter) # {entity: {cluster_id: count}}

        # Iterate over ALL entities (Merchants and Cardholders) and their connections
        # This part still requires knowing immediate neighbors, which we pre-collected in Phase 1
        # If neighbors also don't fit, this would be a map-reduce style join.

        # For cardholders
        for c, merchants_set in cardholder_neighbors_tmp.items():
            current_c_cluster = current_entity_clusters.get(c, c) # Default to self ID if not found
            new_cluster_votes[c][current_c_cluster] += 1 # Self vote
            for m in merchants_set:
                if m in current_entity_clusters:
                    new_cluster_votes[c][current_entity_clusters[m]] += 1

        # For merchants
        for m, cardholders_set in merchant_neighbors_tmp.items():
            current_m_cluster = current_entity_clusters.get(m, m) # Default to self ID if not found
            new_cluster_votes[m][current_m_cluster] += 1 # Self vote
            for c in cardholders_set:
                if c in current_entity_clusters:
                    new_cluster_votes[m][current_entity_clusters[c]] += 1
        
        # Write new cluster assignments
        with open(next_cluster_id_file, 'w', newline='') as f:
            writer = csv.writer(f)
            for entity, votes in new_cluster_votes.items():
                # Assign to the cluster with the highest vote count
                most_common_cluster_id = votes.most_common(1)[0][0]
                writer.writerow([entity, most_common_cluster_id])
        
        current_cluster_id_file = next_cluster_id_file
        print(f"Iteration {iteration + 1} complete. New cluster IDs at {current_cluster_id_file}")
    
    # --- Phase 3: Final Cluster Aggregation and Duplication (Batch Read & Write) ---
    # Read the final cluster assignments and group entities.
    # This phase incorporates duplication logic.

    print("Phase 3: Aggregating final clusters and applying duplication...")

    # Load final entity cluster IDs
    final_entity_clusters = {}
    with open(current_cluster_id_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            final_entity_clusters[row[0]] = row[1]
    
    # Initialize clusters by their assigned IDs
    aggregated_clusters = collections.defaultdict(lambda: {'merchants': set(), 'cardholders': set()})

    # Core assignment (each entity is added to its primary cluster)
    for entity, cluster_id in final_entity_clusters.items():
        if entity.startswith('C'):
            aggregated_clusters[cluster_id]['cardholders'].add(entity)
        else: # Merchant
            aggregated_clusters[cluster_id]['merchants'].add(entity)

    # Duplication logic: iterate through original transactions to find cross-cluster interactions
    # and potentially duplicate entities.
    
    # We still need neighbor information, which was stored in _tmp variables for this run.
    # In a truly out-of-memory scenario, these would be read from their respective temp files.

    # This loop is effectively re-evaluating each entity for duplication based on ALL its links
    # and the final stable cluster assignments.
    for entity_type, entity_neighbors_map in [
        ('C', cardholder_neighbors_tmp),
        ('M', merchant_neighbors_tmp)
    ]:
        for entity_id, connected_entities_set in entity_neighbors_map.items():
            if not connected_entities_set:
                continue

            cluster_interaction_counts = collections.defaultdict(int)
            
            # Count how many connections this entity has to each final cluster
            for connected_entity in connected_entities_set:
                if connected_entity in final_entity_clusters:
                    connected_entity_cluster_id = final_entity_clusters[connected_entity]
                    cluster_interaction_counts[connected_entity_cluster_id] += 1
            
            if not cluster_interaction_counts:
                continue

            sorted_clusters_by_interaction = sorted(
                cluster_interaction_counts.items(), 
                key=lambda item: item[1], 
                reverse=True
            )
            
            primary_cluster_id = sorted_clusters_by_interaction[0][0]
            primary_interactions = sorted_clusters_by_interaction[0][1]
            total_interactions = len(connected_entities_set)

            # Check for duplication to secondary clusters
            for i in range(1, len(sorted_clusters_by_interaction)):
                secondary_cluster_id = sorted_clusters_by_interaction[i][0]
                secondary_interactions = sorted_clusters_by_interaction[i][1]

                if secondary_interactions > 0 and \
                   (float(secondary_interactions) / primary_interactions >= duplication_threshold_ratio or
                    float(secondary_interactions) / total_interactions >= duplication_threshold_ratio):
                    
                    if entity_type == 'C':
                        aggregated_clusters[secondary_cluster_id]['cardholders'].add(entity_id)
                    else: # Merchant
                        aggregated_clusters[secondary_cluster_id]['merchants'].add(entity_id)

    # Write final clusters to individual files or a single structured file
    final_output_file = os.path.join(output_directory, "final_clusters.txt")
    with open(final_output_file, 'w') as outfile:
        for cluster_id, data in aggregated_clusters.items():
            outfile.write(f"Node/Cluster {cluster_id}:\n")
            outfile.write(f"  Merchants: {', '.join(sorted(list(data['merchants'])))}\n")
            outfile.write(f"  Cardholders: {', '.join(sorted(list(data['cardholders'])))}\n")
            outfile.write("-" * 30 + "\n")
    
    print(f"Final clusters written to {final_output_file}")
    
    # Cleanup temporary files (optional)
    for i in range(num_iterations + 1):
        try:
            os.remove(os.path.join(output_directory, f"entity_cluster_id_{i}.csv"))
        except FileNotFoundError:
            pass
    print("Temporary files cleaned up.")


# --- Test Data for Part 3 ---
# Let's create a much larger dummy dataset
num_merchants = 1000
num_cardholders = 5000
num_transactions = 20000

large_test_filepath = "large_transactions.txt"
large_output_dir = "large_cluster_output"

print("\n--- Part 3: Generating Large Test Data ---")
with open(large_test_filepath, "w") as f:
    for _ in range(num_transactions):
        c_id = f"C{random.randint(1, num_cardholders)}"
        m_id = f"M{random.randint(1, num_merchants)}"
        f.write(f"{c_id} {m_id}\n")
print(f"Generated {num_transactions} transactions in {large_test_filepath}")

# Run Part 3
print("\n--- Part 3: Processing Large Dataset (Out-of-Memory Simulation) ---")
process_large_dataset_batch(large_test_filepath, large_output_dir, num_iterations=5, duplication_threshold_ratio=0.2)

# This code simulates how to handle large datasets that don't fit in memory by processing them in batches,
# writing intermediate results to files, and iteratively refining cluster assignments.
# It uses basic Python data structures and file I/O to mimic distributed processing patterns.
# For truly massive datasets, consider using a distributed framework like Apache Spark or Dask.