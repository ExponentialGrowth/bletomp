FROM node:18-alpine

# Install build dependencies for sqlite3 (native addon)
RUN apk add --no-cache python3 make g++

WORKDIR /app

# Copy package files from server directory
COPY package*.json ./

# Install production dependencies
RUN npm install

# Copy server source
COPY tcp_server.js ./

# Expose the API port
EXPOSE 8080

# Command to run the server
CMD ["node", "tcp_server.js"]
