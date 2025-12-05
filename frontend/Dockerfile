FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Clear npm cache and install all dependencies
RUN npm cache clean --force && \
    npm install --legacy-peer-deps && \
    ls -la node_modules/@types/react* || echo "Checking types..." && \
    npm list @types/react @types/react-dom

# Copy source code
COPY . .

# Expose port
EXPOSE 3000

# Set environment variable for React to bind to all interfaces
ENV HOST=0.0.0.0

# Start the app
CMD ["npm", "start"]

