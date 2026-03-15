from __future__ import annotations

import ast
import subprocess
from dataclasses import dataclass
from distutils.ccompiler import new_compiler
from distutils.unixccompiler import UnixCCompiler
from os import path
from pathlib import Path
from platform import system
from tempfile import TemporaryDirectory
from typing import Iterable

from ctypes import c_void_p, cdll
from tree_sitter import Language, Parser


DEFAULT_LANGUAGES = ("python", "sql", "yaml", "javascript", "typescript")


@dataclass(frozen=True)
class GrammarSource:
    repo_url: str
    repo_dir: str
    grammar_dirs: tuple[str, ...]


GRAMMAR_SOURCES: dict[str, GrammarSource] = {
    "python": GrammarSource(
        repo_url="https://github.com/tree-sitter/tree-sitter-python",
        repo_dir="tree-sitter-python",
        grammar_dirs=("tree-sitter-python",),
    ),
    "sql": GrammarSource(
        repo_url="https://github.com/m-novikov/tree-sitter-sql",
        repo_dir="tree-sitter-sql",
        grammar_dirs=("tree-sitter-sql",),
    ),
    "yaml": GrammarSource(
        repo_url="https://github.com/ikatyang/tree-sitter-yaml",
        repo_dir="tree-sitter-yaml",
        grammar_dirs=("tree-sitter-yaml",),
    ),
    "javascript": GrammarSource(
        repo_url="https://github.com/tree-sitter/tree-sitter-javascript",
        repo_dir="tree-sitter-javascript",
        grammar_dirs=("tree-sitter-javascript",),
    ),
    "typescript": GrammarSource(
        repo_url="https://github.com/tree-sitter/tree-sitter-typescript",
        repo_dir="tree-sitter-typescript",
        grammar_dirs=(
            "tree-sitter-typescript/typescript",
            "tree-sitter-typescript/tsx",
        ),
    ),
}


def _clone_grammar(source: GrammarSource, vendor_path: Path) -> None:
    repo_path = vendor_path / source.repo_dir
    if repo_path.exists():
        return

    print(f"Cloning {source.repo_url} into {repo_path}")
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", source.repo_url, str(repo_path)],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Unable to clone {source.repo_url}. "
            "Ensure git is configured and the repository is accessible."
        ) from exc
    except FileNotFoundError as exc:
        raise RuntimeError(
            "git executable is missing; install git to fetch tree-sitter grammars."
        ) from exc


def _ensure_grammars(selected: list[str], vendor_path: Path) -> None:
    vendor_path.mkdir(parents=True, exist_ok=True)
    for lang in selected:
        source = GRAMMAR_SOURCES.get(lang)
        if source is None:
            continue
        _clone_grammar(source, vendor_path)


def _gather_grammar_paths(vendor_path: Path, selected: Iterable[str]) -> list[str]:
    paths: list[str] = []
    for lang in selected:
        source = GRAMMAR_SOURCES.get(lang)
        if source:
            for relative in source.grammar_dirs:
                paths.append(str(vendor_path / relative))
        else:
            paths.append(str(vendor_path / f"tree-sitter-{lang}"))
    return list(dict.fromkeys(paths))


def build_languages(
    vendor_dir: str | Path = "vendor",
    out_path: str | Path = "build/my-languages.so",
    languages: Iterable[str] | None = None,
    force: bool = False,
) -> Path:
    vendor_path = Path(vendor_dir)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not force:
        return out_path

    selected = list(dict.fromkeys(languages or DEFAULT_LANGUAGES))
    _ensure_grammars(selected, vendor_path)
    grammar_paths = _gather_grammar_paths(vendor_path, selected)
    missing = [repo_path for repo_path in grammar_paths if not Path(repo_path).exists()]
    if missing:
        missing_list = ", ".join(missing)
        raise FileNotFoundError(
            f"Missing tree-sitter grammars: {missing_list}. "
            "Clone or stage the directories under vendor/ before building."
        )
    _build_language_library(out_path, grammar_paths)
    return out_path


def load_language(shared_library: str | Path, language_name: str) -> Language:
    try:
        return Language(str(shared_library), language_name)
    except TypeError:
        lib = cdll.LoadLibrary(str(shared_library))
        language_func = getattr(lib, f"tree_sitter_{language_name}", None)
        if language_func is None:
            raise RuntimeError(f"Language {language_name} missing from {shared_library}")
        language_func.restype = c_void_p
        language_id = language_func()
        return Language(language_id)


def make_parser(language: Language) -> Parser:
    parser = Parser()
    # tree_sitter API differs across versions:
    # - newer: `parser.language = language`
    # - older: `parser.set_language(language)`
    try:
        parser.language = language
    except AttributeError:
        parser.set_language(language)
    return parser


def _build_language_library(output_path: Path, repo_paths: list[str]) -> bool:
    output_path = Path(output_path)
    output_mtime = output_path.stat().st_mtime if output_path.exists() else 0

    if not repo_paths:
        raise ValueError("Must provide at least one language folder")

    cpp = False
    source_paths: list[str] = []
    for repo_path in repo_paths:
        src_path = Path(repo_path) / "src"
        parser_c = src_path / "parser.c"
        if parser_c.exists():
            source_paths.append(str(parser_c))
        if (scanner_cc := src_path / "scanner.cc").exists():
            cpp = True
            source_paths.append(str(scanner_cc))
        elif (scanner_c := src_path / "scanner.c").exists():
            source_paths.append(str(scanner_c))

    source_mtimes = [path.getmtime(__file__)] + [path.getmtime(p) for p in source_paths if Path(p).exists()]
    if max(source_mtimes) <= output_mtime:
        return False

    try:
        compiler = new_compiler()
    except Exception as exc:
        raise RuntimeError("Failed to create a C compiler; install setuptools.") from exc

    if isinstance(compiler, UnixCCompiler):
        compiler.set_executables(compiler_cxx="c++")

    with TemporaryDirectory(suffix="tree_sitter_language") as temp_dir:
        object_paths: list[str] = []
        for src_path in source_paths:
            flags = None
            if system() != "Windows":
                flags = ["-fPIC"]
                if src_path.endswith(".c"):
                    flags.append("-std=c11")
            obj = compiler.compile(
                [src_path],
                output_dir=temp_dir,
                include_dirs=[str(Path(src_path).parent)],
                extra_preargs=flags,
            )[0]
            object_paths.append(obj)
        compiler.link_shared_object(
            object_paths,
            str(output_path),
            target_lang="c++" if cpp else "c",
        )
    return True


@dataclass(frozen=True)
class LanguageConfig:
    name: str
    extensions: tuple[str, ...]


LANGUAGE_CONFIGS = (
    LanguageConfig("python", (".py",)),
    LanguageConfig("sql", (".sql",)),
    LanguageConfig("yaml", (".yaml", ".yml")),
    LanguageConfig("javascript", (".js", ".jsx")),
    LanguageConfig("typescript", (".ts",)),
    LanguageConfig("tsx", (".tsx",)),
)


class LanguageRouter:
    """Route files to tree-sitter languages by extension."""

    def __init__(
        self,
        shared_library: str | Path | None = None,
        vendor_dir: str | Path = "vendor",
        out_path: str | Path = "build/my-languages.so",
        languages: Iterable[str] | None = None,
    ) -> None:
        if shared_library is None:
            shared_library = build_languages(
                vendor_dir=vendor_dir,
                out_path=out_path,
                languages=languages,
            )
        self._shared_library = Path(shared_library)
        self._languages = {
            config.name: load_language(self._shared_library, config.name)
            for config in LANGUAGE_CONFIGS
            if languages is None or config.name in set(languages)
        }
        self._extension_map = {
            ext: config.name
            for config in LANGUAGE_CONFIGS
            for ext in config.extensions
            if languages is None or config.name in set(languages)
        }

    def language_for_path(self, path: str | Path) -> Language | None:
        suffix = Path(path).suffix.lower()
        language_name = self._extension_map.get(suffix)
        if not language_name:
            return None
        return self._languages.get(language_name)

    def language_name_for_path(self, path: str | Path) -> str | None:
        suffix = Path(path).suffix.lower()
        return self._extension_map.get(suffix)

    def parser_for_path(self, path: str | Path) -> Parser | None:
        language = self.language_for_path(path)
        if language is None:
            return None
        return make_parser(language)

    def supported_languages(self) -> list[str]:
        return sorted(self._languages.keys())


@dataclass(frozen=True)
class PythonDataAccess:
    datasets: list[str]
    direction: str  # "read" or "write"
    source_file: str


class PythonDataFlowAnalyzer:
    """Walk Python ASTs with tree-sitter to find dataset accesses."""

    DYNAMIC_REFERENCE = "dynamic reference, cannot resolve"
    READ_FUNCTIONS = {"read_csv", "read_sql", "execute", "load"}
    WRITE_FUNCTIONS = {"to_csv", "to_parquet", "save", "write", "insertInto", "saveAsTable"}

    def __init__(self, vendor_dir: str | Path = "vendor", out_path: str | Path = "build/my-languages.so") -> None:
        self.router = LanguageRouter(vendor_dir=vendor_dir, out_path=out_path, languages=["python"])

    def analyze_file(self, path: Path) -> list[PythonDataAccess]:
        parser = self.router.parser_for_path(path)
        if parser is None:
            return []

        source_bytes = path.read_bytes()
        tree = parser.parse(source_bytes)
        return self._collect_calls(tree.root_node, source_bytes, path)

    def _collect_calls(self, node, source_bytes: bytes, path: Path) -> list[PythonDataAccess]:
        accesses: list[PythonDataAccess] = []
        if node.type == "call":
            access = self._extract_call(node, source_bytes, path)
            if access:
                accesses.append(access)

        for child in node.children:
            accesses.extend(self._collect_calls(child, source_bytes, path))
        return accesses

    def _extract_call(self, call_node, source_bytes: bytes, path: Path) -> PythonDataAccess | None:
        function_node = call_node.child_by_field_name("function")
        if function_node is None:
            return None

        full_name = self._node_text(function_node, source_bytes)
        func_name = full_name.split(".")[-1]
        direction = self._classify_direction(func_name)
        if direction is None:
            return None

        datasets = self._resolve_datasets(func_name, call_node, source_bytes)
        if not datasets:
            datasets = [self.DYNAMIC_REFERENCE]

        return PythonDataAccess(datasets=datasets, direction=direction, source_file=str(path))

    def _classify_direction(self, func_name: str) -> str | None:
        if func_name in self.READ_FUNCTIONS:
            return "read"
        if func_name in self.WRITE_FUNCTIONS:
            return "write"
        return None

    def _resolve_datasets(self, func_name: str, call_node, source_bytes: bytes) -> list[str]:
        args = call_node.child_by_field_name("arguments")
        if args is None:
            return []
        for child in args.named_children:
            literal = self._extract_literal(child, source_bytes)
            if literal is not None:
                return self._interpret_literal(func_name, literal)
        return []

    def _interpret_literal(self, func_name: str, literal: str) -> list[str]:
        """
        Convert a literal argument into one or more dataset identifiers.

        - For file-path reads/writes (csv/parquet), keep the literal.
        - For SQL text passed to `read_sql`/`execute`, attempt to parse table refs.
          If parsing fails or sqlglot is unavailable, mark as dynamic/unresolved.
        """
        if func_name in {"read_csv", "to_csv", "to_parquet"}:
            return [literal]

        if self._looks_like_sql(literal):
            tables = self._tables_from_sql(literal)
            return tables or [self.DYNAMIC_REFERENCE]

        return [literal]

    def _looks_like_sql(self, value: str) -> bool:
        head = value.lstrip().lower()
        return head.startswith(("select", "with", "insert", "update", "merge", "delete")) or " from " in head or " join " in head

    def _tables_from_sql(self, sql_text: str) -> list[str]:
        try:
            from sqlglot import parse
            from sqlglot import expressions as exp
        except Exception:
            return []

        for dialect in ("postgres", "bigquery", "snowflake", "duckdb"):
            try:
                statements = parse(sql_text, read=dialect)
            except Exception:
                continue
            tables: set[str] = set()
            for statement in statements:
                if statement is None:
                    continue
                for table in statement.find_all(exp.Table):
                    parts: list[str] = []
                    for attr in ("catalog", "db", "this"):
                        v = getattr(table, attr, None)
                        if v is None:
                            continue
                        parts.append(v.name if hasattr(v, "name") else str(v))
                    identifier = ".".join([p for p in parts if p])
                    if identifier:
                        tables.add(identifier)
            return sorted(tables)

        return []

    def _extract_literal(self, node, source_bytes: bytes) -> str | None:
        if node.type in {"string", "bytes", "raw_string"}:
            return self._evaluate_string(node, source_bytes)
        if node.type == "f_string":
            return self.DYNAMIC_REFERENCE
        for child in node.named_children:
            value = self._extract_literal(child, source_bytes)
            if value is not None:
                return value
        return None

    def _evaluate_string(self, node, source_bytes: bytes) -> str | None:
        text = self._node_text(node, source_bytes)
        try:
            value = ast.literal_eval(text)
            if isinstance(value, str):
                return value
            return str(value)
        except Exception:
            stripped = text.strip("\"'")
            return stripped

    def _node_text(self, node, source_bytes: bytes) -> str:
        return source_bytes[node.start_byte : node.end_byte].decode("utf-8", errors="ignore")
