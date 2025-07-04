FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the transfermkt package directory (required for modular imports)
COPY transfermkt/ /app/transfermkt/

# Copy the application scripts
COPY transfer_mkt_players.py /app/
COPY transfer_mkt_transform.py /app/
COPY transfer_mkt_loader.py /app/
COPY smart_transfer_mkt_loader.py /app/


# Define ENV variables (placeholders)
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

# Default command
CMD ["python", "transfer_mkt_players.py"]