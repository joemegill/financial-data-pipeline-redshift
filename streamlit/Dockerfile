FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the Streamlit requirements file from the build context (project root)
COPY streamlit_requirements.txt /tmp/streamlit_requirements.txt

# Install dependencies using pip
RUN pip install --no-cache-dir -r /tmp/streamlit_requirements.txt

# The application code itself will be mounted at runtime via the volume in docker-compose.yml
# The container is now ready to run the 'streamlit' command.