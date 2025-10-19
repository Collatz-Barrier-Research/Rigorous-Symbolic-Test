import os
import json
import time
from decimal import Decimal

# Import AWS SDK (boto3) and DynamoDB resources
import boto3
from botocore.exceptions import ClientError

# --- Configuration ---
# IMPORTANT: This must match the table name created in DynamoDB
DYNAMO_TABLE_NAME = os.environ.get('DYNAMO_TABLE_NAME', 'TTreeK10')
DYNAMO_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Initialize DynamoDB client (configured via IAM role/environment variables in AWS Batch)
dynamodb = boto3.resource('dynamodb', region_name=DYNAMO_REGION)
table = dynamodb.Table(DYNAMO_TABLE_NAME)

def get_t_tree_node(node_hash: str) -> dict | None:
    """
    Attempts to retrieve a T-Tree node from DynamoDB by its unique hash (Partition Key).
    """
    try:
        response = table.get_item(Key={'id': node_hash})
        return response.get('Item')
    except ClientError as e:
        print(f"Error reading node {node_hash}: {e.response['Error']['Message']}")
        # Implement exponential backoff for retries if necessary
        return None

def put_t_tree_node(node_data: dict, is_new: bool) -> bool:
    """
    Inserts a new node or updates an existing one, ensuring all attributes are valid.
    Uses 'PutItem' for simplicity in this prototype. For large-scale updates,
    'UpdateItem' with conditional logic is more efficient to save WCUs.
    """
    try:
        # DynamoDB does not accept native float, use Decimal for any numerical data
        item_to_put = json.loads(json.dumps(node_data), parse_float=Decimal)

        # Optimization: Use a conditional expression to only write if the node doesn't exist.
        # This saves Write Capacity Units (WCUs) on redundant writes/lookups.
        if is_new:
            table.put_item(
                Item=item_to_put,
                ConditionExpression='attribute_not_exists(id)'
            )
        else:
            table.put_item(Item=item_to_put)
            
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ConditionalCheckFailedException':
             # This is expected if another parallel worker beat this one to the write.
             # We should then re-read the node and merge the results.
             print(f"Conditional write failed for {node_data.get('id')}. Already exists.")
             return False
        
        print(f"Error writing node {node_data.get('id')}: {error_code} - {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

# --- Core Business Logic ---

def calculate_next_step(current_hash: str) -> list[dict]:
    """
    SIMULATION: Represents the expensive process of checking the next 10 digits (prefix)
    and calculating the resulting new structural state and its hash.
    
    In the real application, this function performs the algebraic computation of 
    the Modulus Product and determines the Inclusion Vector for the next T-Tree layer.
    
    Returns a list of potential new nodes to be created/updated.
    """
    # Simulate the heavy computation (0.05 seconds per operation)
    time.sleep(0.05) 
    
    # In k=10, the output should be deterministic based on the input hash/residue
    
    new_nodes = []
    
    # Simulate generating 2 successor states (realistic for a pruned tree)
    for i in range(1, 3): 
        new_hash = f"{current_hash[:5]}_{i}_{int(time.time() * 1000)}"
        new_nodes.append({
            'id': new_hash,
            'k': len(new_hash.split('_')[0]), # The new residue length
            'successor_pointers': [], # Empty for a new node
            'status': 'PENDING',
            'parent_hash': current_hash,
            'residue_operation': f'OP_{i}'
        })
        
    return new_nodes


def process_initial_residue(residue_id: int):
    """
    The main logic for a single AWS Batch job: starts with one of the 1024 residues,
    iteratively builds the T-Tree paths, and writes new nodes to DynamoDB.
    """
    print(f"Starting job for initial residue ID: {residue_id}")
    
    # 1. Define the starting T-Tree node (the root for this branch)
    start_hash = f"R_{residue_id:04d}"
    
    # The queue will contain hashes that need further expansion
    task_queue = [start_hash] 
    
    # 2. Check if the starting node already exists (optional optimization)
    if get_t_tree_node(start_hash) is None:
        initial_node = {
            'id': start_hash,
            'k': 10,
            'successor_pointers': [],
            'status': 'PROCESSING',
            'parent_hash': 'ROOT',
            'residue_operation': 'START'
        }
        put_t_tree_node(initial_node, is_new=True)

    # 3. Iterative T-Tree build (Breadth-First Search logic)
    processed_count = 0
    while task_queue and processed_count < 10000: # Cap the simulation at 10k nodes per job
        current_hash = task_queue.pop(0)
        
        # Calculate the next layer of nodes
        successor_nodes = calculate_next_step(current_hash)
        
        # Log the simulation cost metric
        processed_count += 1
        
        # Batch write the new successor nodes
        newly_added_hashes = []
        for node in successor_nodes:
            # We assume node data is a new insertion (True)
            if put_t_tree_node(node, is_new=True): 
                newly_added_hashes.append(node['id'])
        
        # Add the successfully written new nodes to the queue for expansion
        task_queue.extend(newly_added_hashes)
        
        # Print progress (useful for AWS Batch logs)
        if processed_count % 100 == 0:
            print(f"Residue {residue_id}: Processed {processed_count} nodes. Queue size: {len(task_queue)}")

    # 4. Final step: Mark the starting node as COMPLETE
    table.update_item(
        Key={'id': start_hash},
        UpdateExpression="SET #s = :status",
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={':status': 'COMPLETE'}
    )
    print(f"Job for residue ID {residue_id} completed. Total nodes processed: {processed_count}")


if __name__ == '__main__':
    # AWS Batch job array index is passed as an environment variable
    # The index will range from 0 to 1023 (for 1024 total residues)
    try:
        # AWS Batch passes AWS_BATCH_JOB_ARRAY_INDEX for job arrays
        array_index = int(os.environ.get('AWS_BATCH_JOB_ARRAY_INDEX', 0))
        # We process the residue corresponding to the index + 1
        residue_id_to_process = array_index + 1
        process_initial_residue(residue_id_to_process)
    except Exception as e:
        # Critical error should be logged, which is visible in AWS CloudWatch logs
        print(f"FATAL ERROR during job execution: {e}")
        # Re-raise the exception to signal AWS Batch that the job failed and needs a retry
        raise
