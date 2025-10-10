
import hashlib
from typing import Tuple, Set, Optional, Dict, Any, List

# --- Constants ---
# State structure: (m, d_len, P, r)
STATE_TUPLE = Tuple[int, int, int, int]
PATH_STATS_DICT = Dict[str, Any]

class GMapService:
    """
    A mock class simulating the Global State Map (G-Map) service for the cluster.

    This version includes global state tracking and a specialized mechanism
    for tracking and updating interesting path metadata.
    """

    # Layer 1: Global state for synchronization integrity
    _processed_keys: Set[str] = set()

    # Layer 1: Global metadata for interesting results
    _path_metadata: Dict[str, Any] = {
        'longest_steps': {'steps': 0, 'key': None, 'S_start': None},
        'max_ascent_val': {'ascent': 0.0, 'key': None, 'S_start': None},
        'reported_paths_count': 0
    }

    @staticmethod
    def get_state_key(S: STATE_TUPLE) -> str:
        """
        Generates the unique, fixed-length SHA-256 key for a given symbolic state S.
        """
        s_serialized = f"{S[0]}_{S[1]}_{S[2]}_{S[3]}"
        return hashlib.sha256(s_serialized.encode('utf-8')).hexdigest()

    @classmethod
    def check_and_add_state(cls, S: STATE_TUPLE) -> bool:
        """
        Simulates the critical, atomic cluster synchronization operation (SETNX).
        Returns True if the state is NEW and should be queued for processing.
        """
        state_key = cls.get_state_key(S)
        if state_key in cls._processed_keys:
            return False
        else:
            cls._processed_keys.add(state_key)
            return True

    @classmethod
    def report_path_results(cls, path_key: str, S_start: STATE_TUPLE,
                            total_steps: int, max_cumulative_ascent: float) -> None:
        """
        Cluster nodes call this upon path completion to update the globally
        tracked 'most interesting' results (Layer 1 Metadata).

        Note: In production, this would be an atomic update on the distributed DB.
        """
        cls._path_metadata['reported_paths_count'] += 1

        # 1. Check for Longest Path (Max Steps)
        if total_steps > cls._path_metadata['longest_steps']['steps']:
            cls._path_metadata['longest_steps'] = {
                'steps': total_steps,
                'key': path_key,
                'S_start': S_start
            }

        # 2. Check for Maximum Ascent (Max Valuation Increase)
        if max_cumulative_ascent > cls._path_metadata['max_ascent_val']['ascent']:
            cls._path_metadata['max_ascent_val'] = {
                'ascent': max_cumulative_ascent,
                'key': path_key,
                'S_start': S_start
            }

    @classmethod
    def get_interesting_metadata(cls) -> Dict[str, Any]:
        """Retrieves the globally tracked interesting results."""
        return cls._path_metadata

    @classmethod
    def get_total_unique_states(cls) -> int:
        """Returns the total number of unique states processed so far."""
        return len(cls._processed_keys)

    @classmethod
    def reset_gmap(cls) -> None:
        """Resets the mock map for clean testing."""
        cls._processed_keys.clear()
        cls._path_metadata = {
            'longest_steps': {'steps': 0, 'key': None, 'S_start': None},
            'max_ascent_val': {'ascent': 0.0, 'key': None, 'S_start': None},
            'reported_paths_count': 0
        }


# --- Test Execution ---
if __name__ == '__main__':

    GMapService.reset_gmap()

    # Define mock path results from different cores
    PATH_1_SHORT_FLAT = {'steps': 10, 'ascent': 0.5}
    PATH_2_LONG_ASCENT = {'steps': 50, 'ascent': 15.2}
    PATH_3_VERY_LONG = {'steps': 100, 'ascent': 5.0}

    STATE_X = (1, 1, 1, 1)
    STATE_Y = (2, 2, 2, 2)
    STATE_Z = (3, 3, 3, 3)

    KEY_X = GMapService.get_state_key(STATE_X)
    KEY_Y = GMapService.get_state_key(STATE_Y)
    KEY_Z = GMapService.get_state_key(STATE_Z)

    print("--- G-Map Service Path Reporting Test ---")

    # Core A reports a short, flat path
    GMapService.report_path_results(KEY_X, STATE_X, PATH_1_SHORT_FLAT['steps'], PATH_1_SHORT_FLAT['ascent'])
    print(f"Report 1 (Short): Steps={PATH_1_SHORT_FLAT['steps']}, Ascent={PATH_1_SHORT_FLAT['ascent']}")

    # Core B reports a path with a massive ascent
    GMapService.report_path_results(KEY_Y, STATE_Y, PATH_2_LONG_ASCENT['steps'], PATH_2_LONG_ASCENT['ascent'])
    print(f"Report 2 (Max Ascent): Steps={PATH_2_LONG_ASCENT['steps']}, Ascent={PATH_2_LONG_ASCENT['ascent']}")

    # Core C reports the longest path found so far
    GMapService.report_path_results(KEY_Z, STATE_Z, PATH_3_VERY_LONG['steps'], PATH_3_VERY_LONG['ascent'])
    print(f"Report 3 (Max Steps): Steps={PATH_3_VERY_LONG['steps']}, Ascent={PATH_3_VERY_LONG['ascent']}")

    print("
--- Final Interesting Data Summary ---")
    metadata = GMapService.get_interesting_metadata()

    print(f"Total Paths Reported: {metadata['reported_paths_count']}")

    print("
✅ Longest Path Found:")
    print(f"   Steps: {metadata['longest_steps']['steps']}")
    print(f"   Starting State (S): {metadata['longest_steps']['S_start']}")

    print("
✅ Maximum Ascent Found:")
    print(f"   Max Ascent (Val): {metadata['max_ascent_val']['ascent']}")
    print(f"   Starting State (S): {metadata['max_ascent_val']['S_start']}")
