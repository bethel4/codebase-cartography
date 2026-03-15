from __future__ import annotations

import ast
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import numpy as np
from scipy.cluster.vq import kmeans2

LOGGER = logging.getLogger(__name__)


def _estimate_tokens(text: str) -> int:
    # Very rough heuristic: ~4 chars per token in typical English/code mix.
    return max(1, len(text) // 4)


def _strip_module_docstring(code: str) -> tuple[str, str]:
    """
    Return (code_without_module_docstring, extracted_docstring).

    We remove only the top-level module docstring (if present), not docstrings on
    functions/classes.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code, ""

    doc = ast.get_docstring(tree) or ""
    if not (tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(getattr(tree.body[0], "value", None), ast.Constant)):
        return code, doc

    first = tree.body[0]
    value = first.value
    if not (isinstance(value, ast.Constant) and isinstance(value.value, str)):
        return code, doc

    # Remove the first statement (docstring) using its line span.
    lines = code.splitlines(keepends=True)
    start = getattr(first, "lineno", 1) - 1
    end = getattr(first, "end_lineno", first.lineno) - 1
    if start < 0 or end < start:
        return code, doc
    stripped = "".join(lines[:start] + lines[end + 1 :])
    return stripped, doc


def _extract_top_level_symbols(code: str, max_items: int = 50) -> list[dict[str, Any]]:
    """
    Extract top-level defs (functions/classes) with line numbers for citations.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    symbols: list[dict[str, Any]] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            symbols.append(
                {
                    "kind": "function",
                    "name": node.name,
                    "lineno": getattr(node, "lineno", None),
                    "end_lineno": getattr(node, "end_lineno", None),
                }
            )
        elif isinstance(node, ast.ClassDef):
            symbols.append(
                {
                    "kind": "class",
                    "name": node.name,
                    "lineno": getattr(node, "lineno", None),
                    "end_lineno": getattr(node, "end_lineno", None),
                }
            )
        if len(symbols) >= max_items:
            break
    return symbols


@dataclass
class ContextWindowBudget:
    """
    Tracks token estimates per call to enforce basic budget discipline.

    This is an estimate-only tracker (Ollama does not always report true token counts).
    """

    max_total_tokens: int = 400_000
    total_estimated_tokens: int = 0
    calls: int = 0

    def can_spend(self, tokens: int) -> bool:
        return (self.total_estimated_tokens + tokens) <= self.max_total_tokens

    def spend(self, tokens: int) -> None:
        self.total_estimated_tokens += tokens
        self.calls += 1

    # Spec-friendly aliases
    def estimate_tokens(self, text: str) -> int:
        return _estimate_tokens(text)

    def register_usage(self, tokens: int) -> None:
        self.spend(tokens)

    def can_send(self, tokens: int) -> bool:
        return self.can_spend(tokens)


class OllamaHttpClient:
    """
    Minimal Ollama HTTP client using stdlib urllib.

    Works with:
    - Local Ollama server (default: http://127.0.0.1:11434)
    - Ollama Cloud (set `base_url="https://ollama.com"` and provide an API key)

    For Ollama Cloud, set an API key via `api_key` or environment variable
    `OLLAMA_API_KEY`. The key is sent as `Authorization: Bearer ...`.
    """

    def __init__(self, base_url: str | None = None, timeout_s: int = 300, api_key: str | None = None) -> None:
        # Prefer 127.0.0.1 over localhost to avoid IPv6 ::1 resolution issues when Ollama
        # is bound to 127.0.0.1 only (common on Linux).
        self.base_url = (base_url or os.environ.get("OLLAMA_HOST") or "http://127.0.0.1:11434").rstrip("/")
        self.timeout_s = timeout_s
        self.api_key = api_key or os.environ.get("OLLAMA_API_KEY") or ""

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def chat(self, model: str, messages: list[dict[str, str]], temperature: float = 0.2) -> str:
        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": temperature},
        }
        data = self._post_json("/api/chat", payload)
        message = (data.get("message") or {}).get("content")
        if not isinstance(message, str):
            raise RuntimeError("Unexpected Ollama chat response format")
        return message

    def embeddings(self, model: str, prompt: str) -> list[float]:
        payload = {"model": model, "prompt": prompt}
        data = self._post_json("/api/embeddings", payload)
        embedding = data.get("embedding")
        if not isinstance(embedding, list):
            raise RuntimeError("Unexpected Ollama embeddings response format")
        return [float(x) for x in embedding]

    def ping(self, timeout_s: int = 3) -> bool:
        """
        Quick health check to avoid long timeouts per request.
        """
        url = f"{self.base_url}/api/tags"
        req = Request(url, headers=self._headers(), method="GET")
        try:
            with urlopen(req, timeout=timeout_s) as resp:
                return resp.status == 200
        except Exception:
            return False

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        raw = json.dumps(payload).encode("utf-8")
        req = Request(url, data=raw, headers=self._headers(), method="POST")
        try:
            with urlopen(req, timeout=self.timeout_s) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"Ollama request failed ({exc.code}): {exc.read().decode('utf-8', errors='replace')}") from exc
        except TimeoutError as exc:
            raise RuntimeError(
                f"Ollama request timed out after {self.timeout_s}s. "
                f"Confirm Ollama is running and responsive at {self.base_url} "
                "(try: `curl http://127.0.0.1:11434/api/tags`). "
                "If the model is still loading, retry or increase the timeout."
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"Could not reach Ollama at {self.base_url}. Is it running?") from exc


class OpenRouterHttpClient:
    """
    Minimal OpenRouter HTTP client using stdlib urllib.

    Uses OpenAI-compatible Chat Completions API:
      POST /api/v1/chat/completions

    Notes:
    - OpenRouter model IDs look like: `openai/gpt-4o-mini`, `google/gemini-2.0-flash-001`, etc.
    - Embeddings are optional; if you don't provide an embeddings model, the Semanticist
      will fall back to a local hashing-based embedding for clustering.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://openrouter.ai",
        timeout_s: int = 120,
        title: str = "codebase-cartography",
        referer: str = "http://127.0.0.1",
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.title = title
        self.referer = referer

    def chat(self, model: str, messages: list[dict[str, str]], temperature: float = 0.2) -> str:
        payload = {"model": model, "messages": messages, "temperature": temperature}
        data = self._post_json("/api/v1/chat/completions", payload)
        try:
            return data["choices"][0]["message"]["content"]
        except Exception as exc:
            raise RuntimeError("Unexpected OpenRouter chat response format") from exc

    def embeddings(self, model: str, prompt: str) -> list[float]:
        """
        Best-effort embeddings.

        OpenRouter supports embeddings for some providers/models, but availability varies.
        If this fails, callers should fall back to local embeddings.
        """
        payload = {"model": model, "input": prompt}
        data = self._post_json("/api/v1/embeddings", payload)
        try:
            vec = data["data"][0]["embedding"]
        except Exception as exc:
            raise RuntimeError("Unexpected OpenRouter embeddings response format") from exc
        if not isinstance(vec, list):
            raise RuntimeError("Unexpected OpenRouter embeddings response format")
        return [float(x) for x in vec]

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        raw = json.dumps(payload).encode("utf-8")
        req = Request(
            url,
            data=raw,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "HTTP-Referer": self.referer,
                "X-Title": self.title,
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=self.timeout_s) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"OpenRouter request failed ({exc.code}): {exc.read().decode('utf-8', errors='replace')}") from exc
        except URLError as exc:
            raise RuntimeError(f"Could not reach OpenRouter at {self.base_url}.") from exc


class GeminiHttpClient:
    """
    Minimal Google Gemini API client using stdlib urllib.

    Uses Generative Language API (v1beta):
      POST /v1beta/models/{model}:generateContent?key=...

    Notes:
    - Model IDs look like: `gemini-2.0-flash`, `gemini-2.0-flash-lite`, etc.
    - This client is intentionally small and does not depend on google SDKs.
    """

    def __init__(self, api_key: str, base_url: str = "https://generativelanguage.googleapis.com", timeout_s: int = 120) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s

    def chat(self, model: str, messages: list[dict[str, str]], temperature: float = 0.2) -> str:
        system_text = ""
        contents: list[dict[str, Any]] = []
        for m in messages:
            role = (m.get("role") or "user").lower()
            text = m.get("content") or ""
            if role == "system":
                system_text += (text + "\n")
                continue
            gem_role = "user" if role == "user" else "model"
            contents.append({"role": gem_role, "parts": [{"text": text}]})

        payload: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"temperature": temperature, "responseMimeType": "text/plain"},
        }
        if system_text.strip():
            payload["system_instruction"] = {"parts": [{"text": system_text.strip()}]}

        data = self._post_json(f"/v1beta/models/{model}:generateContent", payload)
        try:
            parts = data["candidates"][0]["content"]["parts"]
            return "".join(p.get("text", "") for p in parts if isinstance(p, dict)).strip()
        except Exception as exc:
            raise RuntimeError("Unexpected Gemini response format") from exc

    def embeddings(self, model: str, prompt: str) -> list[float]:
        """
        Best-effort embeddings via embedContent.

        Model IDs for embeddings are separate from flash chat models; if this call fails,
        callers should fall back to local hashing embeddings.
        """
        payload = {"content": {"parts": [{"text": prompt}]}}
        data = self._post_json(f"/v1beta/models/{model}:embedContent", payload)
        try:
            vec = data["embedding"]["values"]
        except Exception as exc:
            raise RuntimeError("Unexpected Gemini embeddings response format") from exc
        if not isinstance(vec, list):
            raise RuntimeError("Unexpected Gemini embeddings response format")
        return [float(x) for x in vec]

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}?key={self.api_key}"
        raw = json.dumps(payload).encode("utf-8")
        req = Request(url, data=raw, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(req, timeout=self.timeout_s) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"Gemini request failed ({exc.code}): {exc.read().decode('utf-8', errors='replace')}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"Gemini request timed out after {self.timeout_s}s.") from exc
        except URLError as exc:
            raise RuntimeError(f"Could not reach Gemini at {self.base_url}.") from exc


@dataclass(frozen=True)
class ModuleSemanticRecord:
    module_name: str
    path: str
    purpose: str
    docstring_flag: str  # "matches" | "contradicts" | "unknown"
    docstring_reason: str = ""
    doc_drift: bool = False
    doc_similarity: float | None = None
    domain: str = ""
    evidence_symbols: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "module_name": self.module_name,
            "path": self.path,
            "purpose": self.purpose,
            "docstring_flag": self.docstring_flag,
            "docstring_reason": self.docstring_reason,
            "doc_drift": self.doc_drift,
            "doc_similarity": self.doc_similarity,
            "domain": self.domain,
        }
        payload["evidence_symbols"] = self.evidence_symbols or []
        return payload


def _safe_read_text(path: Path, max_chars: int = 30_000) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) > max_chars:
        return text[:max_chars] + "\n# ...(truncated)\n"
    return text


class Semanticist:
    def __init__(
        self,
        client: Any | None = None,
        *,
        bulk_client: Any | None = None,
        synth_client: Any | None = None,
        embed_client: Any | None = None,
        budget: ContextWindowBudget | None = None,
        bulk_model: str = "llama3.1:8b",
        synth_model: str = "llama3.1:70b",
        embed_model: str = "nomic-embed-text",
    ) -> None:
        # Backward compatible: `client` is used for all tasks unless you pass
        # explicit bulk/synth/embed clients.
        self.bulk_client = bulk_client or client
        self.synth_client = synth_client or client
        self.embed_client = embed_client or client
        if self.bulk_client is None or self.synth_client is None:
            raise ValueError("Semanticist requires at least a bulk_client and synth_client (or a single client).")
        self.budget = budget or ContextWindowBudget()
        self.bulk_model = bulk_model
        self.synth_model = synth_model
        self.embed_model = embed_model
        # Runtime availability flags (best-effort). These allow the pipeline to keep running
        # even when an external LLM provider is unavailable or rate-limited.
        self._bulk_llm_available = True
        self._synth_llm_available = True

    @staticmethod
    def _is_rate_limited_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "429" in msg or "resource_exhausted" in msg or "quota exceeded" in msg or "rate limit" in msg

    @staticmethod
    def _is_unavailable_error(exc: Exception) -> bool:
        """
        Best-effort detection of provider unavailability (timeouts / connection issues).

        We use this to stop retrying the bulk provider for every file once it is clearly
        not responding fast enough.
        """
        msg = str(exc).lower()
        return "timed out" in msg or "could not reach" in msg or "connection" in msg or "unreachable" in msg

    @staticmethod
    def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        denom = (np.linalg.norm(a) * np.linalg.norm(b)) or 1.0
        return float(np.dot(a, b) / denom)

    def docstring_similarity(self, docstring: str, purpose: str) -> float | None:
        """
        Compute similarity between docstring and inferred purpose.

        Uses a local hashing embedding (free, deterministic) so it works even when
        provider embeddings are unavailable or expensive.
        """
        if not docstring.strip() or not purpose.strip():
            return None
        vecs = self._local_hash_embeddings([docstring, purpose], dim=256)
        return self._cosine_similarity(vecs[0], vecs[1])

    def _local_hash_embeddings(self, texts: list[str], dim: int = 256) -> np.ndarray:
        mat = np.zeros((len(texts), dim), dtype=np.float32)
        for i, text in enumerate(texts):
            tokens = [t for t in re.split(r"[^a-zA-Z0-9_]+", (text or "").lower()) if t]
            for t in tokens:
                h = int.from_bytes(hashlib.md5(t.encode("utf-8")).digest()[:8], "big")
                mat[i, h % dim] += 1.0
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return mat / norms

    def generate_purpose_statement(
        self,
        module_path: Path,
        code_no_docstring: str,
        *,
        imports: list[str] | None = None,
        imported_by: list[str] | None = None,
        docstring: str = "",
    ) -> str:
        # Keep bulk prompts small to avoid long runtimes on local models.
        code_excerpt = code_no_docstring
        if len(code_excerpt) > 6000:
            code_excerpt = code_excerpt[:6000] + "\n# ...(truncated)\n"
        prompt = (
            "You are a senior data engineer. Write a 2-3 sentence PURPOSE statement for this Python module.\n"
            "Focus on the business/system role (why it exists), not implementation details.\n"
            "Return only the purpose text (no JSON).\n\n"
            f"Module path: {module_path}\n\n"
            f"Imports (best-effort): {', '.join((imports or [])[:25])}\n"
            f"Imported by (best-effort): {', '.join((imported_by or [])[:25])}\n\n"
            "Module docstring (for cross-reference only; do NOT rewrite it):\n"
            f"{docstring.strip()[:2000] if docstring else '(none)'}\n\n"
            "Code excerpt (module docstring removed, truncated if large):\n"
            "```python\n"
            f"{code_excerpt}\n"
            "```\n"
        )
        tokens = _estimate_tokens(prompt)
        if not self.budget.can_spend(tokens):
            raise RuntimeError("ContextWindowBudget exceeded while generating purpose statements.")
        self.budget.spend(tokens)
        return self.bulk_client.chat(self.bulk_model, [{"role": "user", "content": prompt}]).strip()

    def detect_docstring_drift(self, docstring: str, purpose: str, module_path: Path) -> tuple[str, str]:
        if not docstring.strip():
            return "unknown", "No module docstring found."
        prompt = (
            "Decide if the module docstring matches the module purpose.\n"
            "Return exactly two lines:\n"
            "status: matches|contradicts\n"
            "reason: <one sentence>\n\n"
            f"Module: {module_path}\n\n"
            f"Docstring:\n{docstring}\n\n"
            f"Purpose:\n{purpose}\n"
        )
        tokens = _estimate_tokens(prompt)
        if not self.budget.can_spend(tokens):
            return "unknown", "Skipped docstring drift check (budget exceeded)."
        self.budget.spend(tokens)
        raw = self.bulk_client.chat(self.bulk_model, [{"role": "user", "content": prompt}]).strip()
        status = "unknown"
        reason = raw
        for line in raw.splitlines():
            if line.lower().startswith("status:"):
                status = line.split(":", 1)[1].strip().lower()
            elif line.lower().startswith("reason:"):
                reason = line.split(":", 1)[1].strip()
        if status not in {"matches", "contradicts"}:
            status = "unknown"
        return status, reason

    def embed_purposes(self, purposes: list[str]) -> np.ndarray:
        # Prefer provider embeddings if available; otherwise fall back to local hashing embeddings
        # (cheap, deterministic, no API usage).
        if self.embed_model and hasattr(self.embed_client, "embeddings"):
            vectors: list[list[float]] = []
            for purpose in purposes:
                tokens = _estimate_tokens(purpose)
                if not self.budget.can_spend(tokens):
                    raise RuntimeError("ContextWindowBudget exceeded while embedding purposes.")
                self.budget.spend(tokens)
                vectors.append(self.embed_client.embeddings(self.embed_model, purpose))  # type: ignore[attr-defined]
            return np.array(vectors, dtype=np.float32)

        # Local hashing embedding: map tokens -> fixed-dim vector.
        dim = 256
        mat = np.zeros((len(purposes), dim), dtype=np.float32)
        for i, text in enumerate(purposes):
            tokens = [t for t in re.split(r"[^a-zA-Z0-9_]+", (text or "").lower()) if t]
            for t in tokens:
                h = int.from_bytes(hashlib.md5(t.encode("utf-8")).digest()[:8], "big") % dim
                mat[i, h] += 1.0
        # Normalize rows
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return mat / norms

    def cluster_into_domains(self, records: list[ModuleSemanticRecord], k_min: int = 5, k_max: int = 8) -> tuple[list[ModuleSemanticRecord], dict[str, list[str]]]:
        if not records:
            return records, {}

        # Too few modules to meaningfully cluster: use a single domain.
        if len(records) < 3:
            domain = "uncategorized"
            updated = [
                ModuleSemanticRecord(
                    module_name=r.module_name,
                    path=r.path,
                    purpose=r.purpose,
                    docstring_flag=r.docstring_flag,
                    docstring_reason=r.docstring_reason,
                    doc_drift=r.doc_drift,
                    doc_similarity=r.doc_similarity,
                    domain=domain,
                    evidence_symbols=r.evidence_symbols,
                )
                for r in records
            ]
            return updated, {domain: sorted([r.module_name for r in records])}

        purposes = [r.purpose for r in records]
        embeddings = self.embed_purposes(purposes)
        n = embeddings.shape[0]

        # KMeans clustering (algorithmic, no LLM). Use sklearn when available.
        k = int(max(k_min, min(k_max, round(np.sqrt(n)))))
        k = max(2, min(k, n))
        if k >= n:
            k = max(2, n - 1)

        try:
            from sklearn.cluster import KMeans  # type: ignore

            km = KMeans(n_clusters=k, n_init="auto", random_state=42)
            labels = km.fit_predict(embeddings).tolist()
        except Exception:
            # Fallback to SciPy if sklearn isn't available.
            _, labels_arr = kmeans2(embeddings, k, minit="points", iter=20)
            labels = labels_arr.tolist()

        cluster_to_modules: dict[int, list[int]] = {}
        for idx, label in enumerate(labels):
            cluster_to_modules.setdefault(int(label), []).append(idx)

        cluster_names: dict[int, str] = {}
        for cluster_id, member_idxs in cluster_to_modules.items():
            samples = [records[i].purpose for i in member_idxs[:10]]
            prompt = (
                "You are an architect. Infer a short business domain name (1-3 words) for this cluster of module purposes.\n"
                "Return only the domain name.\n\n"
                "Purposes:\n- " + "\n- ".join(samples)
            )
            tokens = _estimate_tokens(prompt)
            if not self.budget.can_spend(tokens):
                cluster_names[cluster_id] = f"domain_{cluster_id}"
                continue
            self.budget.spend(tokens)
            if not self._synth_llm_available:
                cluster_names[cluster_id] = f"domain_{cluster_id}"
                continue
            try:
                name = self.synth_client.chat(self.synth_model, [{"role": "user", "content": prompt}], temperature=0.0).strip()
                name = name.strip().strip('"').strip("'")
                if not name:
                    name = f"domain_{cluster_id}"
                cluster_names[cluster_id] = name
            except Exception as exc:
                if self._is_rate_limited_error(exc):
                    self._synth_llm_available = False
                LOGGER.warning("Domain naming failed (cluster %s): %s", cluster_id, exc)
                cluster_names[cluster_id] = f"domain_{cluster_id}"

        updated: list[ModuleSemanticRecord] = []
        domain_map: dict[str, list[str]] = {}
        for idx, rec in enumerate(records):
            cluster_id = int(labels[idx])
            domain = cluster_names.get(cluster_id, f"domain_{cluster_id}")
            updated_rec = ModuleSemanticRecord(
                module_name=rec.module_name,
                path=rec.path,
                purpose=rec.purpose,
                docstring_flag=rec.docstring_flag,
                docstring_reason=rec.docstring_reason,
                doc_drift=rec.doc_drift,
                doc_similarity=rec.doc_similarity,
                domain=domain,
                evidence_symbols=rec.evidence_symbols,
            )
            updated.append(updated_rec)
            domain_map.setdefault(domain, []).append(rec.module_name)

        for modules in domain_map.values():
            modules.sort()
        return updated, dict(sorted(domain_map.items(), key=lambda x: x[0].lower()))

    def answer_day_one_questions(
        self,
        records: list[ModuleSemanticRecord],
        domain_map: dict[str, list[str]],
        dependency_graph_json: dict[str, Any],
        lineage_graph_json: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Automatically answer the Five FDE Day-One questions from artifacts.

        Design goals:
        - No UI/manual prompting required.
        - Ground answers in the module graph + lineage graph + semanticist module records.
        - Always attach evidence with file paths, line numbers (when available), and whether
          the claim is static analysis vs. LLM inference.
        """

        def record_line_range(rec: ModuleSemanticRecord) -> list[int] | None:
            starts = [s.get("lineno") for s in (rec.evidence_symbols or []) if isinstance(s, dict) and isinstance(s.get("lineno"), int)]
            ends = [s.get("end_lineno") for s in (rec.evidence_symbols or []) if isinstance(s, dict) and isinstance(s.get("end_lineno"), int)]
            if starts and ends:
                return [min(starts), max(ends)]
            return None

        records_by_module = {r.module_name: r for r in records}

        # --- Module graph summaries (static) ---
        dep_nodes = dependency_graph_json.get("nodes") or []
        internal_nodes = [n for n in dep_nodes if isinstance(n, dict) and isinstance(n.get("id"), str) and n.get("path")]
        metrics_by_id: dict[str, dict[str, float]] = {}
        for n in internal_nodes:
            node_id = str(n.get("id"))
            metrics_by_id[node_id] = {
                "pagerank": float(n.get("pagerank") or 0.0),
                "change_velocity_30d": float(n.get("change_velocity_30d") or 0.0),
                "complexity_score": float(n.get("complexity_score") or 0.0),
            }

        critical_modules = sorted(
            (
                {
                    "id": n.get("id"),
                    "path": n.get("path"),
                    "pagerank": float(n.get("pagerank") or 0.0),
                    "change_velocity_30d": float(n.get("change_velocity_30d") or 0.0),
                    "complexity_score": float(n.get("complexity_score") or 0.0),
                }
                for n in internal_nodes
            ),
            key=lambda x: (x["pagerank"], x["complexity_score"], x["change_velocity_30d"]),
            reverse=True,
        )

        most_critical_module = critical_modules[0] if critical_modules else None
        top_velocity = sorted(critical_modules, key=lambda x: x["change_velocity_30d"], reverse=True)[:10]
        top_complexity = sorted(critical_modules, key=lambda x: x["complexity_score"], reverse=True)[:10]

        # --- Lineage graph summaries (static) ---
        lg_nodes = lineage_graph_json.get("nodes") or []
        lg_edges = lineage_graph_json.get("links") or lineage_graph_json.get("edges") or []

        lineage_by_id: dict[str, dict[str, Any]] = {}
        for n in lg_nodes if isinstance(lg_nodes, list) else []:
            if not isinstance(n, dict):
                continue
            nid = n.get("id") or n.get("name")
            if isinstance(nid, str):
                lineage_by_id[nid] = n

        in_deg: dict[str, int] = {}
        out_deg: dict[str, int] = {}
        outgoing_edges: dict[str, list[dict[str, Any]]] = {}
        for e in lg_edges if isinstance(lg_edges, list) else []:
            if not isinstance(e, dict):
                continue
            s = e.get("source")
            t = e.get("target")
            if not isinstance(s, str) or not isinstance(t, str):
                continue
            out_deg[s] = out_deg.get(s, 0) + 1
            in_deg[t] = in_deg.get(t, 0) + 1
            outgoing_edges.setdefault(s, []).append(e)

        def _is_meaningful_dataset_id(node_id: str) -> bool:
            """
            Heuristic filter: lineage extraction can pick up SQL/log strings.
            Prefer ids that look like table/dataset identifiers.
            """
            if not node_id:
                return False
            if len(node_id) > 140:
                return False
            if "\n" in node_id or "\r" in node_id or "\t" in node_id:
                return False
            if node_id.startswith(("f\"", "f'", "b'", "b\"", "#")):
                return False
            if " " in node_id:
                return False
            if any(x in node_id for x in ("{", "}", "(", ")", ";", "\\", "\"", "'")):
                return False

            if not re.match(r"^[A-Za-z0-9_.:-]+$", node_id):
                return False

            # Filter obvious SQL/control-plane tokens that commonly appear in stringified SQL.
            sqlish = {
                "vacuum",
                "checkpoint",
                "install",
                "load",
                "call",
                "drop",
                "create",
                "alter",
                "set",
                "select",
                "insert",
                "update",
                "delete",
            }
            if node_id.lower() in sqlish:
                return False

            # Avoid treating “schema-less” uppercase commands as datasets.
            if node_id.isupper() and "." not in node_id and "_" not in node_id and len(node_id) <= 30:
                return False

            return True

        def _dataset_id_score(node_id: str) -> float:
            """
            Prefer table-like identifiers (schema.table, db.schema.table, etc.) over
            generic tokens (e.g., `tracking_logs`).
            """
            score = 0.0
            if "." in node_id:
                score += 2.0
            if ":" in node_id:
                score -= 0.5
            if "__" in node_id:
                score += 1.5
            if node_id.startswith("_"):
                score -= 0.25
            if node_id.lower().startswith(("temp.", "tmp.")):
                score -= 1.0
            if node_id.lower().startswith("information_schema."):
                score -= 0.75
            return score

        def lineage_sources(limit: int = 10) -> list[str]:
            ids: list[str] = []
            for nid, n in lineage_by_id.items():
                if n.get("kind") not in {"dataset", "table"}:
                    continue
                if in_deg.get(nid, 0) == 0:
                    if not _is_meaningful_dataset_id(nid):
                        continue
                    ids.append(nid)
            ids.sort(key=lambda x: (out_deg.get(x, 0), _dataset_id_score(x), x), reverse=True)
            return ids[:limit]

        def lineage_sinks(limit: int = 10) -> list[str]:
            ids: list[str] = []
            for nid, n in lineage_by_id.items():
                if n.get("kind") not in {"dataset", "table"}:
                    continue
                if out_deg.get(nid, 0) == 0:
                    if not _is_meaningful_dataset_id(nid):
                        continue
                    ids.append(nid)
            ids.sort(key=lambda x: (in_deg.get(x, 0), _dataset_id_score(x), x), reverse=True)
            return ids[:limit]

        srcs = lineage_sources()
        snks = lineage_sinks()

        # --- Domain summaries (LLM inference, but stored as artifacts) ---
        domain_counts = {d: len(v) for d, v in domain_map.items()}

        def evidence_for_module(module_id: str, analysis_method: str) -> dict[str, Any]:
            rec = records_by_module.get(module_id)
            # If semanticist ran, module_id should be workspace-relative path.
            file_path = rec.path if rec else module_id
            return {
                "file_path": file_path,
                "line_numbers": record_line_range(rec) if rec else None,
                "analysis_method": analysis_method,
                "line_numbers_method": "static analysis",
            }

        # Q1: Primary ingestion path (best-effort)
        def ingestion_score(rec: ModuleSemanticRecord) -> float:
            blob = f"{rec.domain} {rec.purpose} {rec.module_name}".lower()
            score = 0.0
            if any(k in blob for k in ("ingest", "ingestion", "extract", "load", "etl", "sync", "sensor")):
                score += 4.0
            if any(k in rec.module_name for k in ("/assets/", "/sensors/", "definitions.py", "/jobs/")):
                score += 2.0
            if rec.module_name.startswith("target_repo/dg_projects/"):
                score += 2.0
            if rec.module_name.startswith("target_repo/dg_deployments/"):
                score += 1.5
            if rec.module_name.startswith("target_repo/bin/"):
                score += 0.75

            # Strongly de-prioritize Superset “serving/admin” modules for ingestion.
            if "/ol_superset/" in rec.module_name or rec.module_name.startswith("target_repo/src/ol_superset/"):
                score -= 10.0
            if any(k in blob for k in ("superset", "dashboard")) and not any(k in blob for k in ("ingest", "extract", "load")):
                score -= 5.0

            m = metrics_by_id.get(rec.module_name, {})
            score += min(5.0, float(m.get("change_velocity_30d") or 0.0))
            score += min(3.0, float(m.get("complexity_score") or 0.0) / 50.0)
            return score

        ingestion_candidates = sorted(records, key=ingestion_score, reverse=True)
        ingestion_mods = [r.module_name for r in ingestion_candidates if ingestion_score(r) > 0][:5]
        if not ingestion_mods:
            ingestion_mods = [r.module_name for r in ingestion_candidates[:5]]

        ingestion_edges = []
        for s in srcs[:3]:
            for e in (outgoing_edges.get(s) or [])[:5]:
                ingestion_edges.append(
                    {
                        "source": e.get("source"),
                        "target": e.get("target"),
                        "transformation_type": e.get("transformation_type"),
                        "source_file": e.get("source_file"),
                        "analysis_method": "static analysis",
                    }
                )

        # Q2: Critical outputs (best-effort from lineage sinks, otherwise from serving-like semantic modules)
        critical_outputs = []
        for nid in snks[:5]:
            node = lineage_by_id.get(nid, {}) or {}
            source_files = node.get("source_files")
            if not source_files:
                # Backfill from inbound edges if node doesn't carry source files.
                inferred = sorted({e.get("source_file") for e in lg_edges if isinstance(e, dict) and e.get("target") == nid and isinstance(e.get("source_file"), str)})
                source_files = inferred or None
            critical_outputs.append(
                {
                    "dataset_id": nid,
                    "kind": node.get("kind"),
                    "source_files": source_files,
                    "analysis_method": "static analysis",
                }
            )
        if len(critical_outputs) < 3:
            serving: list[tuple[float, ModuleSemanticRecord]] = []
            for r in records:
                # `__init__.py` frequently contains minimal glue and is rarely a "critical output" endpoint.
                if r.module_name.endswith("/__init__.py") or r.module_name.endswith("__init__.py"):
                    continue

                blob = f"{r.domain} {r.purpose} {r.module_name}".lower()
                score = 0.0
                if "ol_superset" in r.module_name:
                    score += 6.0
                if any(k in blob for k in ("export", "promote", "dashboard", "report", "sync", "refresh", "validate", "rls", "webhook")):
                    score += 4.0
                if any(k in blob for k in ("serve", "serves", "publishes")):
                    score += 2.0
                if "/assets/" in r.module_name and any(k in blob for k in ("export", "webhook", "publish")):
                    score += 1.5
                if "/commands/" in r.module_name:
                    score += 1.0

                if score <= 0:
                    continue
                serving.append((score, r))

            serving.sort(key=lambda x: (x[0], x[1].module_name), reverse=True)
            for _, r in serving[: max(0, 5 - len(critical_outputs))]:
                critical_outputs.append(
                    {
                        "endpoint_module": r.module_name,
                        "purpose": r.purpose,
                        "analysis_method": "LLM inference",
                    }
                )

        # Q3: Blast radius (static; best-effort from graphs)
        blast = {"most_critical_module": most_critical_module, "limitations": [], "static_impacts": {}}
        if not most_critical_module:
            blast["limitations"].append("No module nodes found in module graph artifact.")
        else:
            mid = str(most_critical_module["id"])
            # Module graph may not resolve dependents as internal targets; we report what we can.
            blast["static_impacts"]["module_graph_import_symbol_edges_present"] = True
            blast["static_impacts"]["note"] = (
                "Module graph edges are primarily file->imported-symbol; internal file-to-file blast radius may be incomplete."
            )
            # Lineage: find edges whose source_file matches this module id/path.
            affected = []
            for e in lg_edges if isinstance(lg_edges, list) else []:
                if not isinstance(e, dict):
                    continue
                if e.get("source_file") == mid:
                    affected.append({"source": e.get("source"), "target": e.get("target"), "transformation_type": e.get("transformation_type")})
            blast["static_impacts"]["lineage_edges_from_module_source_file"] = affected[:25]
            if not affected:
                blast["limitations"].append("No lineage edges tagged with source_file equal to the most critical module id.")

        # Q4: Business logic concentration (static + LLM inference if purposes/domains exist)
        concentration = {
            "top_complexity_modules": top_complexity[:5],
            "top_velocity_modules_30d": top_velocity[:5],
            "domain_counts": domain_counts,
        }

        # Q5: Git velocity (static; limitation note)
        git_velocity = {
            "limitation": "Artifacts provide `change_velocity_30d` only; 90-day velocity requires git history re-analysis.",
            "top_changed_30d": top_velocity[:10],
        }

        # Package answers into the requested skeleton, plus explicit Five-Q section.
        base = {
            "primary_domains": {d: f"{len(v)} modules (domain from semantic clustering/purposes)" for d, v in domain_map.items()},
            "domain_interactions": [],
            "architectural_patterns": [],
            "risks_and_constraints": [
                {
                    "module": "semanticist",
                    "issue": "If `domains` is empty or records are missing, Semanticist did not run successfully on module paths.",
                }
            ],
            "day_one_priorities": [
                {
                    "module": ingestion_mods[0] if ingestion_mods else (most_critical_module["id"] if most_critical_module else ""),
                    "dependencies": [],
                    "evidence": evidence_for_module(ingestion_mods[0], "LLM inference") if ingestion_mods else (evidence_for_module(str(most_critical_module["id"]), "static analysis") if most_critical_module else {}),
                }
            ],
            "fde_questions": {
                "1_primary_data_ingestion_path": {
                    "ingestion_modules": ingestion_mods,
                    "lineage_sources": srcs,
                    "supporting_edges": ingestion_edges,
                    "evidence": [evidence_for_module(m, "LLM inference") for m in ingestion_mods],
                },
                "2_critical_outputs": {
                    "outputs": critical_outputs,
                    "lineage_sinks": snks,
                    "evidence": [{"file_path": ".cartography/lineage_graph.json", "line_numbers": None, "analysis_method": "static analysis"}],
                },
                "3_blast_radius": {
                    "blast_radius": blast,
                    "evidence": [evidence_for_module(str(most_critical_module["id"]), "static analysis")] if most_critical_module else [],
                },
                "4_business_logic_concentration": {
                    "concentration": concentration,
                    "evidence": [{"file_path": ".cartography/module_graph.json", "line_numbers": None, "analysis_method": "static analysis"}],
                },
                "5_git_velocity": {
                    "git_velocity": git_velocity,
                    "evidence": [{"file_path": ".cartography/module_graph.json", "line_numbers": None, "analysis_method": "static analysis"}],
                },
            },
        }
        return self._llm_refine_day_one_answers(base, records, domain_map, dependency_graph_json, lineage_graph_json)

    def _llm_refine_day_one_answers(
        self,
        base_answers: dict[str, Any],
        records: list[ModuleSemanticRecord],
        domain_map: dict[str, list[str]],
        dependency_graph_json: dict[str, Any],
        lineage_graph_json: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Optional LLM refinement step.

        - Keeps the algorithmic, grounded `base_answers` as the source of truth.
        - Asks the synth model to rewrite/summarize/clarify while preserving evidence blocks.
        - If the model fails to return valid JSON, returns `base_answers`.
        """
        if self.synth_client is None:
            return base_answers
        if not self._synth_llm_available:
            return base_answers

        # Keep prompt bounded: do not ship full graphs; provide only base + small context.
        modules_sample = [r.to_dict() for r in records[:50]]
        prompt = (
            "You are an expert codebase analyst.\n"
            "Rewrite the provided BASE_ANSWERS to be clearer and more actionable.\n"
            "Rules:\n"
            "- Return VALID JSON only.\n"
            "- Do NOT remove evidence fields.\n"
            "- Do NOT invent file paths or line numbers.\n"
            "- If something is missing from evidence, keep the limitation.\n\n"
            f"DOMAIN_MAP_COUNTS: {json.dumps({k: len(v) for k, v in domain_map.items()})}\n\n"
            f"MODULE_RECORDS_SAMPLE (for wording only): {json.dumps(modules_sample)[:30000]}\n\n"
            f"BASE_ANSWERS: {json.dumps(base_answers)[:120000]}\n"
        )
        tokens = _estimate_tokens(prompt)
        if not self.budget.can_spend(tokens):
            return base_answers
        self.budget.spend(tokens)
        try:
            raw = self.synth_client.chat(self.synth_model, [{"role": "user", "content": prompt}], temperature=0.1).strip()
            refined = json.loads(raw)
        except Exception:
            return base_answers

        # Ensure required keys exist; otherwise keep base.
        if not isinstance(refined, dict) or "fde_questions" not in refined:
            return base_answers
        return refined

    def run(
        self,
        repo_path: Path,
        module_graph_json: dict[str, Any],
        lineage_graph_json: dict[str, Any],
    ) -> dict[str, Any]:
        nodes = module_graph_json.get("nodes") or []
        internal = [n for n in nodes if isinstance(n, dict) and n.get("path")]
        links = module_graph_json.get("links") or module_graph_json.get("edges") or []
        importers_by_id: dict[str, list[str]] = {}
        if isinstance(links, list):
            for e in links:
                if not isinstance(e, dict):
                    continue
                src = e.get("source")
                tgt = e.get("target")
                if isinstance(src, str) and isinstance(tgt, str):
                    importers_by_id.setdefault(tgt, []).append(src)

        records: list[ModuleSemanticRecord] = []

        # If Ollama is selected for bulk calls but isn't reachable, fall back immediately
        # (so we don't wait for long timeouts on every module).
        if isinstance(self.bulk_client, OllamaHttpClient) and not self.bulk_client.ping(timeout_s=3):
            self._bulk_llm_available = False
            LOGGER.warning("Ollama not reachable at %s; using fallback purposes for this run.", self.bulk_client.base_url)

        for node in internal:
            raw_path = Path(str(node["path"]))
            # Surveyor typically records paths as workspace-relative (e.g., `target_repo/...`).
            # Prefer that directly if it exists; otherwise fall back to repo_path joining.
            if raw_path.is_absolute():
                path = raw_path
            else:
                if raw_path.exists():
                    path = raw_path
                elif (repo_path / raw_path).exists():
                    path = repo_path / raw_path
                else:
                    # Last-chance: if node path already includes repo_path name, try resolving from parent.
                    candidate = repo_path.parent / raw_path
                    if candidate.exists():
                        path = candidate
                    else:
                        continue
            code = _safe_read_text(path, max_chars=20_000)
            code_no_doc, doc = _strip_module_docstring(code)
            node_id = str(node.get("id") or node.get("path") or path)
            imports = [str(x) for x in (node.get("imports") or []) if isinstance(x, str)]
            imported_by = sorted(importers_by_id.get(node_id, []))[:25]

            if self._bulk_llm_available:
                try:
                    purpose = self.generate_purpose_statement(
                        path,
                        code_no_doc,
                        imports=imports,
                        imported_by=imported_by,
                        docstring=doc,
                    )
                except Exception as exc:
                    # If bulk provider is rate-limited or simply not responding quickly enough,
                    # stop retrying per-file and fall back for the rest of the run.
                    if self._is_rate_limited_error(exc) or self._is_unavailable_error(exc):
                        self._bulk_llm_available = False
                    LOGGER.warning("Purpose extraction failed for %s: %s", path, exc)
                    purpose = ""
            else:
                purpose = ""

            if not purpose:
                # Do not crash the full run if LLM is unavailable; fall back to a
                # cheap, deterministic purpose statement so downstream steps (clustering,
                # auto Day-One answers) still work.
                short_imports = ", ".join(imports[:6])
                purpose = f"(fallback) Module at {node_id}. Imports: {short_imports}" if short_imports else f"(fallback) Module at {node_id}."

            try:
                status, reason = self.detect_docstring_drift(doc, purpose, path)
            except Exception as exc:
                status, reason = "unknown", f"Skipped docstring drift check: {exc}"
            sim = self.docstring_similarity(doc, purpose)
            if sim is None and doc.strip() and purpose.strip():
                # Defensive fallback: always provide a similarity score for non-empty strings.
                vecs = self._local_hash_embeddings([doc, purpose], dim=256)
                sim = self._cosine_similarity(vecs[0], vecs[1])
            doc_drift = bool(sim is not None and sim < 0.55)
            symbols = _extract_top_level_symbols(code)
            records.append(
                ModuleSemanticRecord(
                    module_name=node_id,
                    path=str(path),
                    purpose=purpose,
                    docstring_flag=status,
                    docstring_reason=reason,
                    doc_drift=doc_drift,
                    doc_similarity=sim,
                    evidence_symbols=symbols,
                )
            )

        records, domain_map = self.cluster_into_domains(records)

        day_one = self.answer_day_one_questions(
            records,
            domain_map,
            dependency_graph_json=module_graph_json,
            lineage_graph_json=lineage_graph_json,
        )

        return {
            "semanticist": {
                "bulk_model": self.bulk_model,
                "synth_model": self.synth_model,
                "embed_model": self.embed_model,
                "budget": {
                    "max_total_tokens": self.budget.max_total_tokens,
                    "total_estimated_tokens": self.budget.total_estimated_tokens,
                    "calls": self.budget.calls,
                },
            },
            "modules": [r.to_dict() for r in records],
            "domains": domain_map,
            "fde_day_one": day_one,
            "fde_answers": day_one,
            "inputs": {
                "module_graph_nodes": len(module_graph_json.get("nodes") or []),
                "lineage_graph_nodes": len(lineage_graph_json.get("nodes") or []),
            },
        }
