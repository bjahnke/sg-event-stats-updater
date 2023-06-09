# Use the official Python 3.9 image as the base
FROM python:3.9

EXPOSE 8080

# Set the working directory to /app
WORKDIR /app

# Update libpq package
RUN apt-get update && apt-get install -y libpq-dev

# Copy the rest of the application code to the working directory
COPY requirements.txt ./
# Install the dependencies
RUN pip install -r requirements.txt

COPY ./ ./

ENV PYTHONPATH /sg-event-stats-updater

ENTRYPOINT [ "python3" ]
# Set the command to run when the container starts
CMD [ "src/fetch_data/main.py" ]
