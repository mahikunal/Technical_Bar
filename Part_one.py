"""
The find_clusters_simple method is a graph-based approach that looks for strongly connected components in the bipartite graph of merchants and cardholders. It employs a modified Depth-First Search (DFS) algorithm to follow the stream of interactions through the network.

1. The create_interaction_graphs function in the same role as a helper makes two dictionaries here:    
    * cardholder_to_merchants: A graph-based function that assigns each cardholder to the set of merchants they have interacted with.
    * merchant_to_cardholders: This function maps each merchant to the set of cardholders that have transacted with them. These are actually adjacency lists for our bipartite graph.

2. First, visited_cardholders and visited_merchants sets that represent the pool of clusters store that are still to be assigned and that are already assigned to a cluster keep track of elements that have been already used. clusters will still be our answer.

3. The method performs an iteration over all cardholders (then all merchants, for completeness, though cardholders usually cover most cases). If an entity hasn't been visited, it initiates a new DFS traversal.

4. The DFS algorithm:
    * Stack is utilized for DFS traversal.
    * Upon popping a cardholder (c) from stack_cardholders:        
        * This is the current_cluster_cardholders set, and visited is the set of the cardholders that are marked.        
        * All m connected to c are added to current_cluster_merchants. And if these m are not in visited, then all p connected to them are put in stack_cardholders for further exploration. This is how the "connected component" grows.    
        * The same goes further

5. Set formation of the cluster: In this way, after a DFS traversal of the starting entity, all the cardholders and merchants obtained and reached during that traverse form the connected component, and these are then kept as a new cluster.

"""

import collections

def load_transactions(filepath):
    
    # Loads transaction data from a file and returns a list of (cardholder, merchant) tuples.
    transactions = []
    with open(filepath, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                transactions.append((parts[0], parts[1]))
    return transactions

def create_interaction_graphs(transactions):
    """
    Creates adjacency lists for cardholders and merchants.
    cardholder_to_merchants: {C_id: {M_id1, M_id2, ...}}
    merchant_to_cardholders: {M_id: {C_id1, C_id2, ...}}
    """
    cardholder_to_merchants = collections.defaultdict(set)
    merchant_to_cardholders = collections.defaultdict(set)

    for c, m in transactions:
        cardholder_to_merchants[c].add(m)
        merchant_to_cardholders[m].add(c)
    
    return cardholder_to_merchants, merchant_to_cardholders

def find_clusters_simple(transactions):
    """
    Finds clusters of merchants and cardholders based on direct interactions.
    This simple version attempts to find connected components in the bipartite graph.
    It may not perfectly match your desired output for the example due to the
    "no chattiness" being a harder optimization problem, but it will
    group highly interconnected entities.
    """
    cardholder_to_merchants, merchant_to_cardholders = create_interaction_graphs(transactions)

    all_cardholders = set(cardholder_to_merchants.keys())
    all_merchants = set(merchant_to_cardholders.keys())

    # Keep track of visited entities
    visited_cardholders = set()
    visited_merchants = set()

    clusters = []

    # Iterate through all cardholders to ensure all are processed
    for start_cardholder in all_cardholders:
        if start_cardholder in visited_cardholders:
            continue

        current_cluster_cardholders = set()
        current_cluster_merchants = set()
        
        # Use a stack for DFS-like traversal
        stack_cardholders = [start_cardholder]
        
        while stack_cardholders:
            c = stack_cardholders.pop()
            if c in visited_cardholders:
                continue
            
            current_cluster_cardholders.add(c)
            visited_cardholders.add(c)

            # Add all merchants connected to this cardholder
            for m in cardholder_to_merchants[c]:
                if m not in visited_merchants:
                    current_cluster_merchants.add(m)
                    # Add all cardholders connected to this merchant (to explore further)
                    for connected_c in merchant_to_cardholders[m]:
                        if connected_c not in visited_cardholders:
                            stack_cardholders.append(connected_c)
                
        # Now, ensure all merchants in current_cluster_merchants are 'visited' and pull in their cardholders
        # This second pass is crucial for completeness
        stack_merchants = list(current_cluster_merchants)
        while stack_merchants:
            m = stack_merchants.pop()
            if m in visited_merchants:
                continue
            
            visited_merchants.add(m) # Mark as visited only after processing its connections
            
            # Add all cardholders connected to this merchant
            for c_connected_to_m in merchant_to_cardholders[m]:
                if c_connected_to_m not in visited_cardholders:
                    current_cluster_cardholders.add(c_connected_to_m)
                    visited_cardholders.add(c_connected_to_m)
                    stack_cardholders.append(c_connected_to_m) # Add to explore merchants from this cardholder

        # After the DFS-like traversal from the starting cardholder,
        # we have a connected component.
        if current_cluster_cardholders or current_cluster_merchants:
            # Re-collect merchants for the current_cluster_cardholders to ensure all are covered
            final_merchants_in_cluster = set()
            for c in current_cluster_cardholders:
                final_merchants_in_cluster.update(cardholder_to_merchants[c])
            
            clusters.append({
                'merchants': sorted(list(final_merchants_in_cluster)),
                'cardholders': sorted(list(current_cluster_cardholders))
            })
            
    # Handle any isolated merchants not connected to cardholders (less likely given problem context)
    for start_merchant in all_merchants:
        if start_merchant not in visited_merchants:
            current_cluster_cardholders = set()
            current_cluster_merchants = set()
            
            stack_merchants = [start_merchant]
            
            while stack_merchants:
                m = stack_merchants.pop()
                if m in visited_merchants:
                    continue
                
                current_cluster_merchants.add(m)
                visited_merchants.add(m)

                for c in merchant_to_cardholders[m]:
                    if c not in visited_cardholders:
                        current_cluster_cardholders.add(c)
                        for connected_m in cardholder_to_merchants[c]:
                            if connected_m not in visited_merchants:
                                stack_merchants.append(connected_m)

            if current_cluster_cardholders or current_cluster_merchants:
                # Re-collect cardholders for the current_cluster_merchants
                final_cardholders_in_cluster = set()
                for m in current_cluster_merchants:
                    final_cardholders_in_cluster.update(merchant_to_cardholders[m])

                clusters.append({
                    'merchants': sorted(list(current_cluster_merchants)),
                    'cardholders': sorted(list(final_cardholders_in_cluster))
                })

    return clusters

# --- Test Data for Part 1 (Based on your example) ---
# Create a dummy transactions.txt file for testing
test_transactions_content = """
C1 M6
C1 M8
C2 M1
C2 M7
C3 M1
C3 M2
C3 M7
C4 M8
C4 M10
C5 M3
C5 M4
C5 M9
C6 M6
C6 M8
C6 M10
C7 M3
C7 M5
C7 M9
C8 M3
C8 M4
C9 M1
C9 M2
C9 M7
C10 M4
C10 M5
C10 M9
"""

with open("transactions.txt", "w") as f:
    f.write(test_transactions_content.strip())

# Run Part 1
print("--- Part 1: Simple Connected Components ---")
transactions_data = load_transactions("transactions.txt")
initial_clusters = find_clusters_simple(transactions_data)

for i, cluster in enumerate(initial_clusters):
    print(f"Cluster {i+1}:")
    print(f"  Merchants: {cluster['merchants']}")
    print(f"  Cardholders: {cluster['cardholders']}")
    print("-" * 20)