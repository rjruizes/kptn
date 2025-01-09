# Build stage
FROM node:lts-iron AS base
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

FROM base AS build-deps
RUN npm install

FROM base AS dev
COPY --from=build-deps /app/node_modules node_modules
COPY . .
ENV NODE_ENV=development
CMD ["npm", "run", "dev"]

# # Build stage
# FROM base AS build
# RUN npm run build

# # Production stage
# FROM nginx:stable-alpine AS production
# COPY --from=build /app/dist /usr/share/nginx/html
# EXPOSE 80
# CMD ["nginx", "-g", "daemon off;"]