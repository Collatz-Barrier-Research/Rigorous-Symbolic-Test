import hashlib
import heapq
import random
import time
import math
import json
from typing import Tuple, Set, Optional, Dict, Any, List

# --- Constants and Configuration ---
# State structure: (m, d_len, P, r)
STATE_TUPLE = Tuple[int, int, int, int]
K = 3  # The k value for the 2k+1 map (Collatz is k=1, but symbolic is k=3)
SIMULATION_STATES = 10  # Number of random initial states to test the architecture

# --- Cluster Architecture Optimization Constants ---
MAX_PATH_HISTORY_LENGTH = 100 # Max states stored in path_history before truncating (Memory optimization)
MAX_PATH_COMPUTE_TIME_SECONDS = 5.0 # Hard limit to prevent a single complex path from stalling a core
BATCH_SIZE = 500 # Conceptual batch size for G-Map interactions (improves network efficiency)

# --- Contraction Constants ---
CONTRACTION_THRESHOLD_FACTOR = 0.5 # Paths must contract by at least 50% of the expected rate to avoid a flag
EXPECTED_DELTA_VAL_PER_STEP = -1.73 # Expected average contraction per step

# Global State Map (G-Map) Simulation: Stores processed state keys
GLOBAL_STATE_MAP: Set[str] = set()
FLAGGED_ANOMALIES: List[Dict[str, Any]] = []

# Local cache for one cluster node: Avoids costly network lookups for recently seen keys
LOCAL_RECENT_CACHE: Set[str] = set()

# --- Mock GMap Service Implementation (Layer 1: Synchronization & Metadata) ---

class GMapService:
    """
    Simulates the Global State Map service for cluster synchronization and 
    capturing interesting path metadata. Implements local caching for speed.
    """
    
    _processed_keys: Set[str] = GLOBAL_STATE_MAP

    _path_metadata: Dict[str, Any] = {
        'longest_steps': {'steps': 0, 'S_start': None, 'key': None},
        'max_ascent_val': {'ascent': 0.0, 'S_start': None, 'key': None},
        'reported_paths_count': 0
    }

    @staticmethod
    def get_state_key(S: STATE_TUPLE) -> str:
        """Generates the unique, fixed-length SHA-256 key."""
        s_serialized = f"{S[0]}_{S[1]}_{S[2]}_{S[3]}"
        return hashlib.sha256(s_serialized.encode('utf-8')).hexdigest()

    @classmethod
    def check_and_add_state(cls, S: STATE_TUPLE) -> bool:
        """Atomic operation: Returns True if NEW, False if Duplicate. Uses local cache first."""
        state_key = cls.get_state_key(S)
        
        # 1. Check Local Cache (High-speed lookup)
        if state_key in LOCAL_RECENT_CACHE:
            return False

        # 2. Check Global Map (Simulates network call)
        if state_key in cls._processed_keys:
            LOCAL_RECENT_CACHE.add(state_key) # Cache globally found keys locally
            return False
        else:
            # Atomic claim: Add to both global map (integrity) and local cache (speed)
            cls._processed_keys.add(state_key)
            LOCAL_RECENT_CACHE.add(state_key)
            return True

    @classmethod
    def get_total_unique_states(cls) -> int:
        return len(cls._processed_keys)

    @classmethod
    def report_path_results(cls, path_key: str, S_start: STATE_TUPLE, 
                            total_steps: int, max_cumulative_ascent: float) -> None:
        """Updates the globally tracked 'most interesting' results."""
        cls._path_metadata['reported_paths_count'] += 1
        
        if total_steps > cls._path_metadata['longest_steps']['steps']:
            cls._path_metadata['longest_steps'] = {
                'steps': total_steps,
                'S_start': S_start,
                'key': path_key
            }

        if max_cumulative_ascent > cls._path_metadata['max_ascent_val']['ascent']:
            cls._path_metadata['max_ascent_val'] = {
                'ascent': max_cumulative_ascent,
                'S_start': S_start,
                'key': path_key
            }
            
    @classmethod
    def get_interesting_metadata(cls) -> Dict[str, Any]:
        """Retrieves the globally tracked interesting results."""
        return cls._path_metadata

# --- Core Symbolic Math Functions ---

def compute_symbolic_transition(S: STATE_TUPLE, K: int = K) -> List[STATE_TUPLE]:
    """
    MOCK: Simulates the deterministic transition function T(S).
    """
    m, d_len, P, r = S
    
    # Simulate the branching factor (max 40)
    branch_count = random.randint(1, 40)
    successors = []
    
    for _ in range(branch_count):
        m_prime = random.randint(0, 10)
        d_len_prime = d_len + random.randint(-1, 2)
        P_prime = P + random.randint(10, 50)
        r_prime = random.randint(0, 99)
        
        successors.append((m_prime, d_len_prime, P_prime, r_prime))
        
    return successors

def calculate_valuation(m: int, d_len: int) -> float:
    """
    MOCK: Calculates the logarithmic valuation metric Val(S).
    """
    return math.log10(m + 1) + math.log10(d_len + 1) * 2.0


# --- Archiving Function (Final Step) ---

def archive_results(flagged_paths: List[Dict[str, Any]], elapsed_time: float) -> None:
    """
    Writes the final search results and metadata to persistent files.
    """
    print("\n--- ARCHIVING FINAL RESULTS ---")
    
    # 1. Write Anomalies Report (TSV)
    ANOMALY_HEADERS = ["STATE_KEY", "S_START", "ANOMALY_STEP", "VAL_DEVIATION", "PATH_HISTORY_SNIPPET", "NOTE"]
    try:
        with open("anomalies_report.txt", "w") as f:
            f.write("\t".join(ANOMALY_HEADERS) + "\n")
            for anomaly in flagged_paths:
                # Truncate history for file output
                history_str = str(anomaly.get('path_history', ''))[:100] + '...'
                line = [
                    anomaly['key'], 
                    str(anomaly['S_start']), 
                    str(anomaly['total_steps']), 
                    f"{anomaly['val_deviation']:.4f}",
                    history_str,
                    anomaly.get('note', 'Contraction-Stall')
                ]
                f.write("\t".join(line) + "\n")
        print("✅ Anomalies Report (anomalies_report.txt) written successfully.")
    except IOError as e:
        print(f"❌ Error writing anomalies_report.txt: {e}")


    # 2. Write Metadata Report (JSON)
    metadata = GMapService.get_interesting_metadata()
    
    final_metadata = {
        "project_id": "Collatz-Symbolic-T-Tree-k3",
        "completion_date": time.strftime("%Y-%m-%d"),
        "total_unique_states_processed": GMapService.get_total_unique_states(),
        "time_elapsed_seconds": f"{elapsed_time:.2f}",

        "longest_path_found": {
            "metric": "Total Steps",
            "value": metadata['longest_steps']['steps'],
            "S_start": metadata['longest_steps']['S_start'] or "N/A",
            "key": metadata['longest_steps']['key'] or "N/A"
        },
        
        "maximum_ascent_found": {
            "metric": "Max Cumulative Ascent in Val(S)",
            "value": f"{metadata['max_ascent_val']['ascent']:.4f}",
            "S_start": metadata['max_ascent_val']['S_start'] or "N/A",
            "key": metadata['max_ascent_val']['key'] or "N/A"
        },
        
        "anomalies_flagged_count": len(flagged_paths)
    }

    try:
        with open("metadata_report.json", "w") as f:
            json.dump(final_metadata, f, indent=2)
        print("✅ Metadata Report (metadata_report.json) written successfully.")
    except IOError as e:
        print(f"❌ Error writing metadata_report.json: {e}")


# --- Main Search Loop (Layer 2: Execution) ---

def t_tree_search_simulation(initial_states: List[STATE_TUPLE]):
    """
    Simulates the Contraction-Prioritized Parallel Search.
    Pushes only unique states (Layer 1 check) onto a local priority queue.
    """
    start_time = time.time()
    
    # Priority Queue stores: (priority_value, state_tuple, total_steps, cum_delta_val, path_history, start_timestamp)
    # The start_timestamp is used for the hard timeout check.
    search_queue: List[Tuple[float, STATE_TUPLE, int, float, List[STATE_TUPLE], float]] = []
    
    # 1. Initialization: Seed the queue with unique initial states
    print(f"--- Starting T-Tree Search Simulation (Initial Queue: {len(initial_states)} states) ---")
    
    for S_start in initial_states:
        if GMapService.check_and_add_state(S_start): # Check Layer 1 synchronization
            m, d_len, P, r = S_start
            initial_val = calculate_valuation(m, d_len)
            
            # Priority: Negative valuation (low is better), Total Steps: 0, Cumulative Delta Val: 0
            # Start timestamp is current time
            heapq.heappush(search_queue, (-initial_val, S_start, 0, 0.0, [S_start], time.time()))
    
    total_processed = 0
    
    # 2. Main Search Loop
    while search_queue:
        neg_val_priority, S_current, total_steps, cum_delta_val, path_history, start_timestamp = heapq.heappop(search_queue)
        
        total_processed += 1
        current_val = -neg_val_priority # Recalculate true valuation
        
        # --- Check for Hard Time-Out (Mitigation for unexpected complexity) ---
        if time.time() - start_timestamp > MAX_PATH_COMPUTE_TIME_SECONDS:
            print(f"⚠️ Hard Timeout! Path from {path_history[0]} exceeded {MAX_PATH_COMPUTE_TIME_SECONDS}s and was terminated.")
            FLAGGED_ANOMALIES.append({
                'key': GMapService.get_state_key(S_current),
                'S_start': path_history[0],
                'total_steps': total_steps,
                'cum_delta_val': cum_delta_val,
                'val_deviation': 999.0, # High deviation to mark as critical failure
                'path_history': path_history,
                'note': 'TERMINATED_BY_TIMEOUT'
            })
            continue # Terminate this path and move to the next item in queue

        # --- Check for Terminal State (Contraction to known cycle) ---
        if S_current[0] == 0 and S_current[1] == 0:
            # Path terminated successfully. Report results for metadata tracking.
            max_ascent = max(calculate_valuation(s[0], s[1]) for s in path_history if isinstance(s, tuple)) - current_val 
            GMapService.report_path_results(
                GMapService.get_state_key(S_current), 
                path_history[0], 
                total_steps, 
                max_ascent
            )
            continue # Path closed, continue to next item in queue

        # --- Check for Anomalous Path (Layer 2: Dynamic Contraction Threshold) ---
        expected_contraction = total_steps * EXPECTED_DELTA_VAL_PER_STEP
        anomaly_threshold = expected_contraction * CONTRACTION_THRESHOLD_FACTOR
        
        if cum_delta_val > anomaly_threshold and total_steps > 0:
            FLAGGED_ANOMALIES.append({
                'key': GMapService.get_state_key(S_current),
                'S_start': path_history[0],
                'total_steps': total_steps,
                'cum_delta_val': cum_delta_val,
                'val_deviation': cum_delta_val - anomaly_threshold,
                'path_history': path_history,
                'note': 'Contraction-Stall'
            })
            # Even if flagged, we continue processing to prove the path terminates.

        # --- Status Reporting ---
        if total_processed % 500 == 0:
            elapsed = time.time() - start_time
            print(f"Processed: {total_processed} | Queue Size: {len(search_queue)} | Current Step: {total_steps} | Time: {elapsed:.2f}s")


        # --- Generate Successors ---
        successors = compute_symbolic_transition(S_current, K)
        
        for S_prime in successors:
            if GMapService.check_and_add_state(S_prime): # Layer 1 Check (Local Cache -> G-Map)
                # New State: Calculate priority and push to queue
                m_prime, d_len_prime, P_prime, r_prime = S_prime
                val_prime = calculate_valuation(m_prime, d_len_prime)
                
                # Calculate contraction for this step
                delta_val_step = val_prime - current_val
                new_cum_delta_val = cum_delta_val + delta_val_step
                new_total_steps = total_steps + 1
                
                new_path_history = path_history + [S_prime]
                
                # Path History Truncation (Memory Optimization)
                if len(new_path_history) > MAX_PATH_HISTORY_LENGTH:
                    new_path_history = [new_path_history[0]] + ["...TRUNCATED..."] + new_path_history[-MAX_PATH_HISTORY_LENGTH+2:]
                
                # Push back with new priority (negative valuation) and original start timestamp
                heapq.heappush(search_queue, (-val_prime, S_prime, new_total_steps, new_cum_delta_val, new_path_history, start_timestamp))


    # 3. Finalization and Archiving
    elapsed_time = time.time() - start_time
    print("\n--- Simulation Complete ---")
    print(f"Total Unique States Claimed Globally: {GMapService.get_total_unique_states()}")
    print(f"Total States Processed Locally: {total_processed}")
    print(f"Time Elapsed: {elapsed_time:.2f} seconds")
    print(f"Flagged Anomalous Paths: {len(FLAGGED_ANOMALIES)}")

    # Final archiving step
    archive_results(FLAGGED_ANOMALIES, elapsed_time)


# --- Test Setup ---
if __name__ == '__main__':
    # Initial states are derived from residues (r=1 to 99) and simplified P/m/d_len
    MOCK_INITIAL_STATES: List[STATE_TUPLE] = [
        (random.randint(1, 5), 10, random.randint(1, 100), r)
        for r in random.sample(range(1, 100), SIMULATION_STATES)
    ]
    t_tree_search_simulation(MOCK_INITIAL_STATES)
