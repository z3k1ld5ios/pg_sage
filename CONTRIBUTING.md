# Contributing to pg_sage

Thanks for your interest in pg_sage! We welcome contributions of all kinds.

## Ways to Contribute

- **Bug reports** — Found something broken? Open an issue with steps to reproduce.
- **Feature requests** — Have an idea? Open an issue and describe the use case.
- **Code contributions** — PRs welcome. See below for guidelines.
- **Documentation** — Improvements to README, spec, or inline comments are always appreciated.
- **Testing** — Run pg_sage against your workloads and report findings (good or bad).

## Development Setup

```bash
git clone https://github.com/jasonmassie01/pg_sage.git
cd pg_sage
docker compose up
```

The Docker environment includes PostgreSQL 17 with all dependencies pre-configured.

## Code Guidelines

- pg_sage is written in C as a native PostgreSQL extension
- Follow existing code style in `src/`
- All new features need test coverage in `test/`
- Run the full test suite before submitting: `docker exec -i pg_sage-pg_sage-1 psql -U postgres < test/test_all_features.sql`

## Pull Request Process

1. Fork the repo and create a feature branch
2. Write your changes with tests
3. Ensure all tests pass
4. Submit a PR with a clear description of what and why

## Code of Conduct

Be respectful, be constructive, be kind. We're all here to make Postgres better.

## License

By contributing, you agree that your contributions will be licensed under AGPL-3.0.
