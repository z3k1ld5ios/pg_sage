FROM postgres:17-bookworm

# Build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-17 \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy extension source
COPY . /build/pg_sage/
WORKDIR /build/pg_sage

# Build and install
RUN make clean && make && make install

# Configure PostgreSQL to load pg_sage
RUN echo "shared_preload_libraries = 'pg_stat_statements,pg_sage'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "sage.database = 'postgres'" >> /usr/share/postgresql/postgresql.conf.sample

# Custom entrypoint script
COPY docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/
