FROM apache/airflow:2.10.1

# 1. Ensure root for privileged operations
USER root 

# 2. Privileged operation: Copying file and setting ownership for the 'airflow' user
COPY requirements.txt /requirements-airflow.txt

# 3. CRITICAL FIX: Explicitly set read permissions (644) for the requirements file.
# This ensures the owner (airflow) can definitely read it.
RUN chmod 644 /requirements-airflow.txt

# 4. Switch to the non-root 'airflow' user to satisfy Airflow's requirement.
USER airflow

# 5. Installation: Now runs as the 'airflow' user and has explicit read permission.
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Note: The user remains 'airflow' for the rest of the build and runtime.