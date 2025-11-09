# Contributing to Retail Analytics Solution

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-org/retail_analytics_solution.git
   cd retail_analytics_solution
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install pre-commit hooks**
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Branching Strategy

- `main`: Production-ready code, deployed to staging
- `develop`: Integration branch for development
- `feature/*`: Feature branches
- `bugfix/*`: Bug fix branches
- `hotfix/*`: Emergency production fixes

## Making Changes

1. **Create a feature branch**
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write clear, concise code
   - Follow PEP 8 style guidelines
   - Add docstrings to functions and classes
   - Update documentation as needed

3. **Write tests**
   - Add unit tests for new functionality
   - Ensure all tests pass
   - Maintain or improve code coverage

4. **Run quality checks**
   ```bash
   # Format code
   black src/ tests/
   isort src/ tests/
   
   # Lint code
   ruff check src/ tests/
   
   # Run tests
   pytest tests/
   
   # Type checking
   mypy src/
   ```

5. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

   Use conventional commit messages:
   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation changes
   - `test:` Test additions or modifications
   - `refactor:` Code refactoring
   - `chore:` Maintenance tasks

6. **Push and create pull request**
   ```bash
   git push origin feature/your-feature-name
   ```
   
   Then create a pull request on GitHub targeting the `develop` branch.

## Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Ensure CI pipeline passes
- Request review from at least one team member
- Address review comments promptly
- Keep PRs focused and reasonably sized

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Maximum line length: 100 characters
- Use meaningful variable and function names
- Add comments for complex logic

## Testing Guidelines

- Write tests for all new functionality
- Aim for >80% code coverage
- Include both positive and negative test cases
- Use pytest fixtures for common test setup
- Mock external dependencies

## Documentation

- Update README.md for significant changes
- Add docstrings to all public functions and classes
- Document configuration changes
- Update API documentation if applicable

## Databricks-Specific Guidelines

### Notebooks

- Use magic commands appropriately
- Include markdown cells for documentation
- Test notebooks before committing
- Keep notebooks focused on single tasks

### Asset Bundles

- Validate bundle configuration before committing
- Test in development environment first
- Document any new resources
- Follow naming conventions

### Jobs and Pipelines

- Use appropriate cluster configurations
- Set reasonable timeouts and retry policies
- Add email notifications for failures
- Tag resources appropriately

## Review Process

1. Automated checks must pass (CI pipeline)
2. At least one approval required
3. No unresolved conversations
4. Up-to-date with target branch

## Questions?

If you have questions, please:
- Check existing documentation
- Search closed issues
- Ask in team chat
- Create a discussion on GitHub

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
