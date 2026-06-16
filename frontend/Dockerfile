FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Clear npm cache and install all dependencies
RUN npm cache clean --force && \
    npm install --legacy-peer-deps && \
    npm list @types/react @types/react-dom ajv ajv-keywords || true

# Copy source code
COPY . .

# Expose port
EXPOSE 3000

# Set environment variable for React to bind to all interfaces
ENV HOST=0.0.0.0

# Build version stamp (git SHA). CRA inlines REACT_APP_* when `npm start` compiles
# at container boot, so the footer reads process.env.REACT_APP_GIT_SHA. Defaults to "dev".
ARG GIT_SHA=dev
ENV REACT_APP_GIT_SHA=$GIT_SHA

# Start the app
CMD ["npm", "start"]

