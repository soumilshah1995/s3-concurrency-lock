import boto3
import time
import json
import argparse


class S3Lock:
    def __init__(self, bucket_name, lock_name, concurrency_limit, counter_name="active_locks.json"):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name
        self.lock_name = lock_name
        self.concurrency_limit = concurrency_limit
        self.counter_name = counter_name
        self.lock_path = f"locks/{lock_name}"
        self.counter_path = counter_name

    def acquire_lock(self, wait_time=10, retry_interval=1):
        start_time = time.time()
        while time.time() - start_time < wait_time:
            if self._check_concurrency_limit():
                try:
                    self.s3.put_object(Bucket=self.bucket_name, Key=self.lock_path, Body='')
                    self._increment_active_locks()
                    print(f"Lock '{self.lock_name}' acquired.")
                    return True
                except Exception:
                    pass  # Lock is already held; retry
            time.sleep(retry_interval)

        print(f"Failed to acquire lock '{self.lock_name}' after waiting {wait_time} seconds.")
        return False

    def release_lock(self):
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=self.lock_path)
            self._decrement_active_locks()
            print(f"Lock '{self.lock_name}' released.")
        except Exception as e:
            print(f"Error releasing lock: {e}")

    def _check_concurrency_limit(self):
        active_locks = self._get_active_locks()
        return active_locks < self.concurrency_limit

    def _get_active_locks(self):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=self.counter_path)
            content = response['Body'].read().decode('utf-8')
            return int(json.loads(content)['count'])
        except Exception:
            return 0

    def _increment_active_locks(self):
        active_locks = self._get_active_locks()
        new_count = active_locks + 1
        self._update_active_locks(new_count)

    def _decrement_active_locks(self):
        active_locks = self._get_active_locks()
        new_count = max(0, active_locks - 1)
        self._update_active_locks(new_count)

    def _update_active_locks(self, count):
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=self.counter_path, Body=json.dumps({'count': count}))
        except Exception as e:
            print(f"Error updating active lock count: {e}")


def worker(lock, lock_name):
    if lock.acquire_lock(wait_time=30, retry_interval=2):
        try:
            print(f"Doing some work for job: {lock_name}")
            time.sleep(12)  # Replace with your actual job logic
            print(f"Work finished for job: {lock_name}")
        finally:
            lock.release_lock()
    else:
        print(f"Could not acquire lock for job: {lock_name}, job will not run.")


# Main Function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3-based lock with concurrency control.")
    parser.add_argument("--job_name", required=True, help="The name of the job (used as the lock name).")
    args = parser.parse_args()

    # Configuration
    BUCKET_NAME = "<BUCKET>"  # Replace with your S3 bucket name
    LOCK_NAME = args.job_name  # Use the job name from the command line argument
    CONCURRENCY_LIMIT = 1  # Allow up to 1 concurrent job

    # Initialize the lock
    lock = S3Lock(BUCKET_NAME, LOCK_NAME, CONCURRENCY_LIMIT)

    # Call the worker function to execute the job
    worker(lock, LOCK_NAME)
