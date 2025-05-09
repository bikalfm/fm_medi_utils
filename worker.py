# In your worker application (conceptual example)
import aioredis # type: ignore
import json
import datetime
import uuid # For worker ID or if task ID isn't in the dequeued message
import os # For worker ID from env, for example

# --- Configuration (match your FastAPI app's settings.py) ---
REDIS_URL = os.getenv("REDIS_URL", "redis://34.28.202.243:6379/0") # Use your actual Redis URL
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "context_api_task_queue")
REDIS_ACTIVE_TASKS_SET_KEY = os.getenv("REDIS_ACTIVE_TASKS_SET_KEY", "active_processing_tasks")
REDIS_COMPLETED_TASKS_LIST_KEY = os.getenv("REDIS_COMPLETED_TASKS_LIST_KEY", "completed_tasks_history")
REDIS_HISTORY_MAX_LENGTH = int(os.getenv("REDIS_HISTORY_MAX_LENGTH", "1000"))

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}" # Example worker ID

async def process_task_from_queue():
    redis = await aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    print(f"Worker {WORKER_ID} started, listening to queue: {REDIS_QUEUE_NAME}")

    while True:
        try:
            # Blocking pop from the queue (right end)
            # BRPOP returns a tuple (queue_name, task_json_string) or None on timeout
            raw_task_message = await redis.brpop(REDIS_QUEUE_NAME, timeout=0) # 0 for indefinite block
            
            if raw_task_message is None:
                continue

            _queue_name, task_json_string = raw_task_message
            
            # 1. Deserialize task
            try:
                task_data = json.loads(task_json_string)
                if not isinstance(task_data, dict): # Basic validation
                    print(f"Error: Task data is not a dictionary: {task_json_string[:100]}")
                    # Potentially log to a dead-letter queue or error log
                    continue
            except json.JSONDecodeError:
                print(f"Error: Could not decode task JSON: {task_json_string[:100]}")
                # Potentially log to a dead-letter queue or error log
                continue

            task_id = task_data.get("task_id", f"unknown_task_{uuid.uuid4().hex[:6]}")
            print(f"Worker {WORKER_ID} picked up task: {task_id}")

            # 2. Add to Active Tasks Set in Redis
            active_task_payload = {
                **task_data, # Original task data (includes queued_at if API added it)
                "worker_id": WORKER_ID,
                "started_at": datetime.datetime.utcnow().isoformat(),
            }
            active_task_json = json.dumps(active_task_payload)
            await redis.sadd(REDIS_ACTIVE_TASKS_SET_KEY, active_task_json)
            print(f"Worker {WORKER_ID} marked task {task_id} as active.")

            # --- SIMULATE ACTUAL TASK PROCESSING ---
            processing_result = None
            processing_error = None
            try:
                print(f"Worker {WORKER_ID} processing task {task_id} of type {task_data.get('task_type')}...")
                # Replace this with your actual task processing logic
                # For example:
                # if task_data.get("task_type") == "process_repository_file":
                #     result = await actual_file_processing_function(task_data.get("file_path"))
                #     processing_result = {"summary": f"Processed {task_data.get('file_path')}", "output_size": len(result)}
                await asyncio.sleep(5) # Simulate work
                processing_result = {"message": f"Task {task_id} completed successfully by {WORKER_ID}"}
                task_status = "completed"
                print(f"Worker {WORKER_ID} finished processing task {task_id}.")
            except Exception as e:
                print(f"Worker {WORKER_ID} ERROR processing task {task_id}: {e}")
                processing_error = str(e)
                task_status = "failed"
            # --- END SIMULATED PROCESSING ---

            # 3. Remove from Active Tasks Set
            await redis.srem(REDIS_ACTIVE_TASKS_SET_KEY, active_task_json) # Use the same JSON string
            print(f"Worker {WORKER_ID} removed task {task_id} from active set.")

            # 4. Add to Completed Tasks History List (LPUSH to add to the head)
            completed_task_payload = {
                **active_task_payload, # Includes worker_id, started_at, original task data
                "status": task_status,
                "finished_at": datetime.datetime.utcnow().isoformat(),
                "result": processing_result,
                "error_message": processing_error,
            }
            completed_task_json = json.dumps(completed_task_payload)
            await redis.lpush(REDIS_COMPLETED_TASKS_LIST_KEY, completed_task_json)
            # Trim the list to keep it from growing indefinitely
            await redis.ltrim(REDIS_COMPLETED_TASKS_LIST_KEY, 0, REDIS_HISTORY_MAX_LENGTH - 1)
            print(f"Worker {WORKER_ID} logged task {task_id} to history with status: {task_status}.")

        except aioredis.RedisError as e:
            print(f"Worker {WORKER_ID} Redis error: {e}. Reconnecting attempt implied by library.")
            await asyncio.sleep(5) # Wait before retrying connection/operation
        except Exception as e:
            print(f"Worker {WORKER_ID} encountered an unexpected error: {e}")
            await asyncio.sleep(1) # Brief pause before next attempt

    # Clean up (though this loop is infinite)
    # await redis.close()

if __name__ == "__main__":
    import asyncio
    # To run multiple workers, you'd typically run this script multiple times
    # or use a process manager like Supervisor, or orchestrate with Celery/RQ.
    try:
        asyncio.run(process_task_from_queue())
    except KeyboardInterrupt:
        print("Worker shutting down...")
