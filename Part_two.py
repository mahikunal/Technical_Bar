"""
This method tries to become more complicated with a feature that allows the entities to be shared among various clusters.

1. Initial Core Cluster Formation (DFS-like):    
    * It starts by searching for unassigned entities, giving first priority to those with more connections (as they are likely to be part of a denser core).
    * For a new "core" cluster growth it execution the breadth-first search (BFS, through collections.deque).    
    * entity_to_core_cluster_id: This dictionary is the most important. It links an entity to the first cluster which it was given as a core member. After an entity is a core member of one cluster, it is considered "assigned" in terms of creating new clusters, but, however, it can still be duplicated into other clusters later.    
    * The unassigned_cardholders and unassigned_merchants sets prevent each entity from starting a core cluster more than once.

2. Duplication Phase: This is the part that is new and most important.
    * final_clusters: The clusters, which were initially formed, are copied since these will be altered in the process of introducing the duplicated entities.    
    * Iterate Entities: It goes over all merchants and cardholders again.    
    * Count Interactions per Cluster: For a given entity_id (e.g., C1), it goes over all of its connected_entity (e.g., M1, M2, M3). Then, for each connected_entity, it decides to which of the initially formed clusters (final_clusters) that connected_entity belongs. Further, it adds one to the counter (cluster_interaction_counts) of that cluster. This informs of the number of connections of entity_id that are within each existing cluster.    
    * Primary Assignment: The entity_id is by default inserted into the cluster where with it has the greatest number of interactions (its primary_cluster_id).    
    * Duplication Logic: The next step is it going through the other clusters one by one. If the number of the interactions with a secondary_cluster is:
        * Above a certain duplication_threshold_ratio compared to the primary cluster's interactions, OR
        * Above a certain duplication_threshold_ratio compared to its total interactions,
    * The entity_id is duplicated into that secondary cluster.
    
"""

import collections
import random
import Part_one  #Part_one.py contains the necessary functions

# Reuse load_transactions and create_interaction_graphs from Part 1

def find_clusters_with_duplication(transactions, duplication_threshold_ratio=0.3):
    """
    Finds clusters of merchants and cardholders, allowing for strategic duplication
    based on a heuristic to minimize inter-cluster chattiness.

    Args:
        transactions (list): List of (cardholder, merchant) tuples.
        duplication_threshold_ratio (float): If an entity has more than this ratio
                                            of its total interactions with a secondary
                                            cluster (compared to its primary cluster),
                                            it may be duplicated. A lower value means more duplication.

    Returns:
        list: A list of dictionaries, where each dict represents a cluster
              with 'merchants' and 'cardholders' sets.
    """
    cardholder_to_merchants, merchant_to_cardholders = Part_one.create_interaction_graphs(transactions)

    all_cardholders = set(cardholder_to_merchants.keys())
    all_merchants = set(merchant_to_cardholders.keys())

    # Keep track of which entities are considered 'core' in a cluster
    # An entity can be a 'core' member of only one cluster, but can be 'duplicated' in others
    entity_to_core_cluster_id = {} # {entity_id: cluster_id}
    clusters = [] # List of {'merchants': set(), 'cardholders': set()}

    unassigned_cardholders = set(all_cardholders)
    unassigned_merchants = set(all_merchants)

    # Heuristic: Prioritize starting new clusters with entities that have many connections
    # to form a strong core.
    sorted_cardholders = sorted(all_cardholders, key=lambda c: len(cardholder_to_merchants[c]), reverse=True)
    sorted_merchants = sorted(all_merchants, key=lambda m: len(merchant_to_cardholders[m]), reverse=True)

    # Combine and shuffle for a somewhat random but connectivity-aware start
    start_candidates = sorted_cardholders + sorted_merchants
    random.shuffle(start_candidates) # Shuffle to avoid bias in large graph

    while unassigned_cardholders or unassigned_merchants:
        # Find a starting point for a new cluster
        current_seed = None
        for entity in start_candidates:
            if entity.startswith('C') and entity in unassigned_cardholders:
                current_seed = entity
                break
            elif entity.startswith('M') and entity in unassigned_merchants:
                current_seed = entity
                break
        
        if current_seed is None: # All assigned or no more unassigned entities
            break

        # Initialize a new cluster
        current_cluster_id = len(clusters)
        current_cluster_merchants = set()
        current_cluster_cardholders = set()
        
        # Entities to explore in the current cluster expansion
        queue = collections.deque([current_seed])

        while queue:
            entity = queue.popleft()

            if entity.startswith('C'): # It's a cardholder
                if entity in entity_to_core_cluster_id and entity_to_core_cluster_id[entity] != current_cluster_id:
                    # Already a core member of another cluster, consider as a bridge
                    continue
                
                if entity not in current_cluster_cardholders:
                    current_cluster_cardholders.add(entity)
                    if entity in unassigned_cardholders: # Only mark as core if unassigned
                        entity_to_core_cluster_id[entity] = current_cluster_id
                        unassigned_cardholders.discard(entity)
                    
                    for merchant in cardholder_to_merchants[entity]:
                        if merchant not in current_cluster_merchants:
                            queue.append(merchant)

            elif entity.startswith('M'): # It's a merchant
                if entity in entity_to_core_cluster_id and entity_to_core_cluster_id[entity] != current_cluster_id:
                    # Already a core member of another cluster, consider as a bridge
                    continue

                if entity not in current_cluster_merchants:
                    current_cluster_merchants.add(entity)
                    if entity in unassigned_merchants: # Only mark as core if unassigned
                        entity_to_core_cluster_id[entity] = current_cluster_id
                        unassigned_merchants.discard(entity)

                    for cardholder in merchant_to_cardholders[entity]:
                        if cardholder not in current_cluster_cardholders:
                            queue.append(cardholder)

        # After core cluster formation, add it to the list
        if current_cluster_merchants or current_cluster_cardholders:
            clusters.append({
                'merchants': current_cluster_merchants,
                'cardholders': current_cluster_cardholders
            })
    
    # --- Duplication Phase ---
    # Now, iterate through all entities and see if they should be duplicated
    # The current 'clusters' only contain core assignments.
    final_clusters = [ {'merchants': set(c['merchants']), 'cardholders': set(c['cardholders'])} for c in clusters]

    for entity_type, all_entities, interaction_map in [
        ('C', all_cardholders, cardholder_to_merchants),
        ('M', all_merchants, merchant_to_cardholders)
    ]:
        for entity_id in all_entities:
            # Skip if entity has no interactions
            if not interaction_map[entity_id]:
                continue
            
            # Count interactions with each existing cluster
            cluster_interaction_counts = collections.defaultdict(int)
            
            for connected_entity in interaction_map[entity_id]:
                # Find which clusters 'connected_entity' belongs to (as core or duplicated)
                for i, cluster in enumerate(final_clusters):
                    if (entity_type == 'C' and connected_entity in cluster['merchants']) or \
                       (entity_type == 'M' and connected_entity in cluster['cardholders']):
                        cluster_interaction_counts[i] += 1
            
            # Determine best cluster and potential for duplication
            if not cluster_interaction_counts:
                continue # Entity has no connections to any formed cluster (isolated, which should be caught by core logic)

            sorted_clusters_by_interaction = sorted(
                cluster_interaction_counts.items(), 
                key=lambda item: item[1], 
                reverse=True
            )
            
            primary_cluster_id = sorted_clusters_by_interaction[0][0]
            primary_interactions = sorted_clusters_by_interaction[0][1]
            total_interactions = len(interaction_map[entity_id])

            # Add to primary cluster (if not already there)
            if entity_type == 'C':
                final_clusters[primary_cluster_id]['cardholders'].add(entity_id)
            else: # Merchant
                final_clusters[primary_cluster_id]['merchants'].add(entity_id)

            # Check for duplication
            for i in range(1, len(sorted_clusters_by_interaction)):
                secondary_cluster_id = sorted_clusters_by_interaction[i][0]
                secondary_interactions = sorted_clusters_by_interaction[i][1]

                # Heuristic: If secondary interactions are significant compared to primary, duplicate
                # Or, if secondary interactions represent a significant portion of total interactions
                if secondary_interactions > 0 and \
                   (float(secondary_interactions) / primary_interactions >= duplication_threshold_ratio or
                    float(secondary_interactions) / total_interactions >= duplication_threshold_ratio):
                    
                    if entity_type == 'C':
                        final_clusters[secondary_cluster_id]['cardholders'].add(entity_id)
                    else: # Merchant
                        final_clusters[secondary_cluster_id]['merchants'].add(entity_id)

    # Convert sets to sorted lists for consistent output
    for cluster in final_clusters:
        cluster['merchants'] = sorted(list(cluster['merchants']))
        cluster['cardholders'] = sorted(list(cluster['cardholders']))

    return final_clusters

# --- Test Data for Part 2 ---
# Using the same test_transactions_content as Part 1, as the problem describes
# how that matrix can be transformed into clusters with duplication.
# The `duplication_threshold_ratio` might need tuning to achieve your exact example output.

# Let's create a slightly more complex dataset to demonstrate duplication
# Scenario: M1, M2 are core, but C1 interacts with both and C1 also interacts with M3
# C1 should ideally be in a cluster with M1, M2. If M3 is in another cluster, C1 might be duplicated.
complex_test_data = """
C1 M1
C1 M2
C1 M3
C2 M1
C2 M4
C3 M2
C3 M5
C4 M3
C4 M5
C5 M6
C5 M7
"""
with open("complex_transactions.txt", "w") as f:
    f.write(complex_test_data.strip())

print("\n--- Part 2: Clustering with Duplication (Original Example Data) ---")
transactions_data_part2 = Part_one.load_transactions("transactions.txt")
# Tweak duplication_threshold_ratio to see its effect.
# A higher threshold means less duplication, a lower threshold means more.
clusters_with_duplication = find_clusters_with_duplication(transactions_data_part2, duplication_threshold_ratio=0.4)

for i, cluster in enumerate(clusters_with_duplication):
    print(f"Cluster {i+1}:")
    print(f"  Merchants: {cluster['merchants']}")
    print(f"  Cardholders: {cluster['cardholders']}")
    print("-" * 20)

print("\n--- Part 2: Clustering with Duplication (Complex Test Data) ---")
transactions_data_complex = Part_one.load_transactions("complex_transactions.txt")
clusters_complex = find_clusters_with_duplication(transactions_data_complex, duplication_threshold_ratio=0.3)

for i, cluster in enumerate(clusters_complex):
    print(f"Cluster {i+1}:")
    print(f"  Merchants: {cluster['merchants']}")
    print(f"  Cardholders: {cluster['cardholders']}")
    print("-" * 20)