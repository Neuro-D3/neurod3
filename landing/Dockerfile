# Placeholder landing page for datareuse.org, served as static files.
# Cloud Run sends traffic to $PORT (8080 by default); nginx listens there.
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
COPY assets/ /usr/share/nginx/html/assets/
EXPOSE 8080
