Pre-Commit Hooks:
```bash
pip install pylint and git-pylint-commit-hook
```
and add the pre-commit hook:
```bash
#!/bin/sh
git-pylint-commit-hook
```
to
`.git/hooks/pre-commit`
