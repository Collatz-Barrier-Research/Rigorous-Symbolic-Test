import os
import json
import time
import hashlib
import random
from datetime import datetime, timezone

# Boto3 is the AWS SDK for Python
import boto3
from botocore.exceptions import ClientError

# --- Configuration and Initialization ---

# Fetch environment variables
REGION_NAME = os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
DYNAMO_TABLE_NAME = os.environ.get('DYNAMO_TABLE_NAME')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

# Initialize clients and resources
try:
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    sqs = boto3.client('sqs', region_name=REGION_NAME)
except Exception as e:
    print(f"FATAL: Failed to initialize AWS clients. Check credentials and region settings. Error: {e}")
    exit(1)

print(f"Worker initialized for Table: {DYNAMO_TABLE_NAME} in Region: {REGION_NAME}")

# --- Helper Functions for Node Management (Finalized ID and Depth Logic) ---

def calculate_hash(data):
    """
    Calculates a consistent SHA-256 hash for a given data dictionary (the actual content).
    This hash should ONLY be based on the immutable, algebraic properties of the node.
    """
    # Ensure consistent order by sorting keys before dumping to JSON
    dumped_data = json.dumps(data, sort_keys=True)
    return hashlib.sha256(dumped_data.encode('utf-8')).hexdigest()

def generate_node_id(parent_node_id):
    """
    Generates a unique Node ID. Uses '|' to reliably separate the parent ID from
    the new unique suffix, allowing for accurate depth calculation.
    """
    # Use nanoseconds + random for maximum collision avoidance
    timestamp_ns = time.time_ns()
    rand_suffix = random.randint(1000, 9999) 
    
    if parent_node_id is None:
        # Should only be used if root ID fails, but kept for robustness.
        return f"R_ROOT|{timestamp_ns}_{rand_suffix}" 
    
    # CRITICAL: Use '|' to denote a level change.
    # We append the new unique suffix to the parent ID.
    return f"{parent_node_id}|{timestamp_ns}_{rand_suffix}"

def get_node_depth(node_id: str) -> int:
    """
    Robustly calculates the depth of the node based on the number of split markers.
    
    Depth 1: R_0001 (Root)
    Depth 2: R_0001|timestamp_rand
    Depth 3: R_0001|timestamp_rand|timestamp_rand
    """
    # If the ID uses the OLD format (only '_'), we treat it as an expanded child 
    # and conservatively set its depth to MAX_DEPTH to stop expansion.
    if '|' not in node_id and node_id.count('_') > 1:
        # This catches old, deep nodes like R_1_17609... and effectively prunes them.
        return 999 

    # For the correct format (and the simple R_0001 root):
    # Count the number of split markers ('|') and add 1 for the root level.
    # The root node 'R_0001' has 0 pipes, so 0 + 1 = Depth 1.
    return node_id.count('|') + 1

def write_node(node_id, parent_id, content):
    """Writes or updates a node in DynamoDB. Assumes NodeId is the Partition Key."""
    
    # 1. Create the base data structure
    node_data = {
        # *** NodeId is the DynamoDB Partition Key (PK) ***
        'NodeId': node_id, 
        'ParentId': parent_id if parent_id else 'ROOT',
        'Content': content,
        'Depth': get_node_depth(node_id), # Now uses the fixed depth calculation
        'CreationTimestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # 2. Calculate the content hash 
    node_hash = calculate_hash(content)
    node_data['NodeHash'] = node_hash 

    # 3. Write to DynamoDB
    try:
        # Ensure we use the 'NodeId' (Partition Key) for conditional write
        table.put_item(
            Item=node_data,
            ConditionExpression='attribute_not_exists(NodeId)'
        )
        print(f"Successfully wrote node: {node_id[:20]}... (Depth: {node_data['Depth']}) with Content Hash: {node_hash[:10]}...")
        return node_hash
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ConditionalCheckFailedException':
            print(f"Conditional write failed for {node_id[:20]}.... Node already exists.")
            return node_hash 
        
        print(f"Error writing node {node_id[:20]}...: {error_code} - {e.response['Error']['Message']}")
        return None
    except Exception as e:
        print(f"Error writing node {node_id[:20]}...: {e}")
        return None

def send_sqs_job(node_id, parent_id, data):
    """Sends a job message to the SQS queue."""
    message_body = {
        'node_id': node_id,
        'parent_id': parent_id,
        'data': data
    }
    
    try:
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        # Only print the start of the ID for cleaner logs
        return True
    except ClientError as e:
        print(f"Error queuing job for {node_id[:20]}...: {e.response['Error']['Message']}")
        return False

# --- Core Logic ---

def process_job(message):
    """Processes a single job message received from SQS."""
    # Set MAX_DEPTH low to ensure quick termination during testing/cleanup.
    MAX_DEPTH = 10 
    
    try:
        job = json.loads(message['Body'])
        node_id = job.get('node_id')
        parent_id = job.get('parent_id')
        data = job.get('data')

        if not node_id or not parent_id or not data:
            print("Job message is missing required fields. Deleting message.")
            return True 

        # Print only the start of the ID for cleaner logs
        print(f"Processing node starting with: {node_id[:20]}... Parent starting with: {parent_id[:20]}...")

        # 1. Write the new or updated node (this will calculate and store the depth)
        new_hash = write_node(node_id, parent_id, data)
        current_depth = get_node_depth(node_id)
        
        if new_hash:
            # --- T-Tree Stop Condition ---
            if current_depth >= MAX_DEPTH:
                # Log the short ID for confirmation
                print(f"Node {node_id[:20]}... (Depth {current_depth}) reached MAX_DEPTH ({MAX_DEPTH}). Stopping expansion.")
                return True # Successfully processed, but no new children queued.
            
            # 2. Simulate node split/expansion: create and queue two new child jobs
            child1_id = generate_node_id(node_id)
            child2_id = generate_node_id(node_id)
            
            # Queue child 1
            send_sqs_job(child1_id, node_id, f"Split Data A from {node_id[:10]}...")
            # Queue child 2
            send_sqs_job(child2_id, node_id, f"Split Data B from {node_id[:10]}...")

            print(f"Job for {node_id[:20]}... complete. Children jobs queued.")
            return True # Successful processing
            
        return False # Failed to write node

    except json.JSONDecodeError:
        print("Received message is not valid JSON. Deleting message.")
        return True 
    except Exception as e:
        print(f"An unexpected error occurred during job processing: {e}")
        return False

def process_initial_residue(residue_id):
    """
    Handles the special case of the first node ('R_0001') which starts the tree.
    """
    initial_content = f"Initial T-Tree Root Node for Residue {residue_id}. Structure: K_10"
    # The first root node uses '_' for consistency with initial setup
    root_id = f"R_{residue_id:04d}" 
    
    print(f"Starting job for initial residue ID: {residue_id}")
    
    # 1. Write the initial root node item
    root_hash = write_node(root_id, None, initial_content)

    if not root_hash:
        print("FATAL ERROR: Failed to write initial root node. Aborting.")
        return None, None

    # 2. Simulate updating the root node's status/metadata (using UpdateItem)
    try:
        table.update_item(
            Key={
                'NodeId': root_id 
            },
            UpdateExpression="SET #s = :status, LastUpdated = :timestamp",
            ExpressionAttributeNames={
                '#s': 'Status'
            },
            ExpressionAttributeValues={
                ':status': 'INITIALIZED',
                ':timestamp': datetime.now(timezone.utc).isoformat()
            }
        )
        print(f"Successfully updated root node: {root_id}")

        return root_id, root_hash

    except ClientError as e:
        print(f"FATAL ERROR during job execution: {e.response['Error']['Message']}")
        return None, None
    except Exception as e:
        print(f"FATAL ERROR during job execution: {e}")
        return None, None


def poll_sqs_for_jobs():
    """Main loop to poll the SQS queue for new jobs."""
    print("Worker is now polling SQS queue for jobs...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=300 
            )

            messages = response.get('Messages', [])
            if not messages:
                print("No messages received. Waiting...")
                time.sleep(10) # Wait a bit longer if no messages
                continue

            print(f"Received {len(messages)} messages. Processing...")

            for message in messages:
                if process_job(message):
                    # Delete message only on successful processing
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                else:
                    print(f"Failed to process message {message.get('MessageId', 'unknown')}. It will be retried.")

        except ClientError as e:
            if e.response['Error']['Code'] in ['AWS.SimpleQueueService.NonExistentQueue', 'AccessDenied']:
                print(f"CRITICAL SQS ERROR: {e.response['Error']['Message']}")
                time.sleep(60) 
            else:
                print(f"SQS Client Error: {e}")
                time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred during SQS polling: {e}")
            time.sleep(5)

# --- Main Execution ---

if __name__ == "__main__":
    residue_id_to_process = os.environ.get('AWS_BATCH_JOB_ARRAY_INDEX')
    if residue_id_to_process is not None:
        try:
            residue_id = int(residue_id_to_process) + 1
        except ValueError:
            residue_id = 1
    else:
        residue_id = 1 

    # 1. Create and initialize the root node (R_1)
    root_id, root_hash = process_initial_residue(residue_id)
    
    if root_id and root_hash:
        # 2. Enqueue the first two child jobs immediately after root initialization
        print("Kicking off the first SQS jobs...")
        child1_id = generate_node_id(root_id)
        child2_id = generate_node_id(root_id)
        
        # We start the first two jobs with "PENDING" content to be processed by the SQS loop
        send_sqs_job(child1_id, root_id, "PENDING_JOB_A")
        send_sqs_job(child2_id, root_id, "PENDING_JOB_B")

    # 3. Start the main SQS polling loop
    poll_sqs_for_jobs()
