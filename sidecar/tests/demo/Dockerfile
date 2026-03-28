FROM postgres:17

# Install HypoPG (hypothetical indexes) and pg_hint_plan (query hints).
# auto_explain ships as a contrib module in the base image.
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
       postgresql-17-hypopg \
       postgresql-17-pg-hint-plan \
    && rm -rf /var/lib/apt/lists/*
