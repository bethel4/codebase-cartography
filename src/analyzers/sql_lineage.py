from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from sqlglot import parse
from sqlglot import expressions as exp

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SQLDependency:
    target: str
    sources: list[str]
    source_file: Path


class SQLLineageAnalyzer:
    DIALECTS = ("postgres", "bigquery", "snowflake", "duckdb")
    _jinja_block = re.compile(r"{%.*?%}", re.DOTALL)
    _jinja_expr = re.compile(r"{{.*?}}", re.DOTALL)
    _ref_macro = re.compile(r"\{\{\s*ref\(\s*['\"](?P<name>[^'\"]+)['\"][^)]*\)\s*\}\}")
    _source_macro = re.compile(
        r"\{\{\s*source\(\s*['\"](?P<schema>[^'\"]+)['\"]\s*,\s*['\"](?P<table>[^'\"]+)['\"][^)]*\)\s*\}\}"
    )
    _this_macro = re.compile(r"\{\{\s*this\s*\}\}")
    _dialect_hint = re.compile(r"(?m)^\s*(?:--|#)\s*dialect\s*:\s*(?P<dialect>\w+)")
    _dialect_keywords: dict[str, set[str]] = {
        "snowflake": {"snowflake"},
        "bigquery": {"bigquery", "bq"},
        "postgres": {"postgres", "postgresql"},
        "duckdb": {"duckdb"},
    }

    def analyze_file(self, path: Path) -> list[SQLDependency]:
        compiled_path = self._compiled_sql_path(path)
        source_path = compiled_path or path
        if compiled_path:
            LOGGER.debug("Using compiled SQL for %s", path)
        text = source_path.read_text(encoding="utf-8", errors="ignore")
        if not text.strip():
            return []

        text = self._strip_jinja(text)
        text = self._fix_missing_alias_expressions(text)

        statements = self._parse_statements(text, source_path)
        dependencies: list[SQLDependency] = []
        for statement in statements:
            if statement is None:
                LOGGER.debug("Skipping empty parse result for %s", source_path)
                continue
            dependencies.extend(self._statement_dependencies(statement, path))
        return dependencies

    def analyze_directory(self, root: Path) -> list[SQLDependency]:
        if not root.exists():
            return []

        dependencies: list[SQLDependency] = []
        for path in sorted(root.rglob("*.sql")):
            dependencies.extend(self.analyze_file(path))
        return dependencies

    def _statement_dependencies(self, statement: exp.Expression, path: Path) -> list[SQLDependency]:
        dependencies: list[SQLDependency] = []
        targets = self._extract_targets(statement)
        cte_targets = self._collect_cte_targets(statement)
        sources = self._extract_sources(statement, targets + cte_targets)
        for target in targets:
            dependencies.append(SQLDependency(target=target, sources=sources, source_file=path))
        dependencies.extend(self._cte_dependencies(statement, path))
        return dependencies

    def _parse_statements(self, text: str, path: Path) -> Iterable[exp.Expression]:
        last_error: Exception | None = None
        preferred = self._dialects_from_context(path, text)
        dialects_to_try = list(dict.fromkeys(preferred + list(self.DIALECTS)))
        for dialect in dialects_to_try:
            try:
                statements = parse(text, read=dialect)
            except Exception as exc:  # pragma: no cover - best effort parsing
                last_error = exc
                LOGGER.debug("Failed to parse %s as %s: %s", path, dialect, exc)
                continue
            LOGGER.debug("Parsed %s using %s dialect", path, dialect)
            return statements
        if last_error:
            LOGGER.warning(
                "Could not parse SQL in %s after trying %s: %s",
                path,
                dialects_to_try,
                last_error,
            )
        return []

    def _extract_targets(self, statement: exp.Expression) -> list[str]:
        targets: list[str] = []
        if isinstance(statement, (exp.Create, exp.Insert, exp.Merge, exp.Update)) and isinstance(statement.this, exp.Table):
            identifier = self._table_identifier(statement.this)
            if identifier:
                targets.append(identifier)
        return targets

    def _collect_cte_targets(self, statement: exp.Expression) -> list[str]:
        with_expr = statement.args.get("with")
        if not hasattr(statement, "args"):
            return []
        if not with_expr:
            return []
        return [self._alias_name(cte.alias) for cte in with_expr.expressions if self._alias_name(cte.alias)]

    def _cte_dependencies(self, statement: exp.Expression, path: Path) -> list[SQLDependency]:
        dependencies: list[SQLDependency] = []
        with_expr = statement.args.get("with")
        if not hasattr(statement, "args") or not with_expr:
            return dependencies
        for cte in with_expr.expressions:
            alias = self._alias_name(cte.alias)
            if not alias or not getattr(cte, "this", None):
                continue
            sources = self._extract_sources(cte.this, [])
            if sources:
                dependencies.append(SQLDependency(target=alias, sources=sources, source_file=path))
        return dependencies

    def _extract_sources(self, statement: exp.Expression, excluded: list[str]) -> list[str]:
        sources: set[str] = set()
        for table in statement.find_all(exp.Table):
            identifier = self._table_identifier(table)
            if identifier and identifier not in excluded:
                sources.add(identifier)
        return sorted(sources)

    def _table_identifier(self, table: exp.Table) -> str | None:
        parts: list[str] = []
        for attribute in ("catalog", "db", "this"):
            value = getattr(table, attribute, None)
            if value is None:
                continue
            if hasattr(value, "name"):
                parts.append(value.name)
            else:
                parts.append(str(value))
        identifier = ".".join(filter(None, parts))
        return identifier or None

    def _strip_jinja(self, text: str) -> str:
        text = self._source_macro.sub(self._replace_source_macro, text)
        text = self._ref_macro.sub(self._replace_ref_macro, text)
        text = self._this_macro.sub("this", text)
        text = self._jinja_block.sub(" ", text)
        text = self._jinja_expr.sub(" ", text)
        return text

    def _fix_missing_alias_expressions(self, text: str) -> str:
        """
        Best-effort cleanup for common invalid SQL produced by templating/stripping:

        - ",  as alias"  -> ", NULL as alias"
        - "select  as alias" (line starts with `as`) -> "select NULL as alias"

        This is intentionally conservative: it only patches the specific "missing
        expression before AS" cases that otherwise cause sqlglot to hard-fail.
        """
        # In select lists, missing expression before `AS` is often preceded by a comma.
        text = re.sub(r"(?i),\s*as\s+([a-zA-Z_][\\w$]*)", r", NULL as \\1", text)
        # Also handle cases where a line begins with `as alias` (after macro stripping).
        text = re.sub(r"(?im)^\\s*as\\s+([a-zA-Z_][\\w$]*)", r"  NULL as \\1", text)
        return text

    def _dialects_from_context(self, path: Path, text: str) -> list[str]:
        hints: list[str] = []
        hint_match = self._dialect_hint.search(text)
        if hint_match:
            dialect = hint_match.group("dialect").lower()
            if dialect in self.DIALECTS:
                hints.append(dialect)
        for part in (p.lower() for p in path.parts):
            for dialect, keywords in self._dialect_keywords.items():
                if dialect in hints:
                    continue
                if any(keyword in part for keyword in keywords):
                    hints.append(dialect)
        return hints

    @staticmethod
    def _replace_ref_macro(match: re.Match[str]) -> str:
        name = match.group("name")
        return SQLLineageAnalyzer._normalize_identifier(name)

    @staticmethod
    def _replace_source_macro(match: re.Match[str]) -> str:
        schema = SQLLineageAnalyzer._normalize_identifier(match.group("schema"))
        table = SQLLineageAnalyzer._normalize_identifier(match.group("table"))
        if schema and table:
            return f"{schema}.{table}"
        return schema or table or "source"

    @staticmethod
    def _normalize_identifier(value: str) -> str:
        cleaned = value.strip().strip('"').strip("'")
        return cleaned

    def _compiled_sql_path(self, path: Path) -> Path | None:
        project_dir = self._find_project_dir(path)
        if not project_dir:
            return None
        project_file = project_dir / "dbt_project.yml"
        if not project_file.exists():
            return None
        project_name = self._project_name(project_file)
        if not project_name:
            return None
        compiled_root = project_dir / "target" / "compiled" / project_name
        try:
            relative = path.relative_to(project_dir)
        except ValueError:
            return None
        candidate = compiled_root / relative
        if candidate.exists():
            return candidate
        return None

    def _find_project_dir(self, path: Path) -> Path | None:
        current = path if path.is_dir() else path.parent
        while True:
            if (current / "dbt_project.yml").exists():
                return current
            if current.parent == current:
                return None
            current = current.parent

    @staticmethod
    def _project_name(project_file: Path) -> str | None:
        try:
            for raw_line in project_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw_line.split("#", 1)[0].strip()
                if not line:
                    continue
                match = re.match(r"name\s*:\s*(?P<name>.+)", line)
                if match:
                    return match.group("name").strip().strip('"').strip("'")
        except OSError:
            return None
        return None

    def _alias_name(self, node) -> str | None:
        if node is None:
            return None
        if isinstance(node, exp.Identifier):
            return node.name
        if isinstance(node, exp.Table):
            return self._table_identifier(node)
        if isinstance(node, exp.Alias) and node.this:
            return self._alias_name(node.this)
        if hasattr(node, "this"):
            return self._alias_name(node.this)
        return None
