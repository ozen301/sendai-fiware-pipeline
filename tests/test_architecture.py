import ast
from pathlib import Path

_RUNNER_MODULES = {
    "sendai_pipeline.run_direction",
    "sendai_pipeline.run_flow",
}


def _qualified_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        owner = _qualified_name(node.value)
        if owner is not None:
            return f"{owner}.{node.attr}"
    return None


def _private_runner_dependencies(tree: ast.AST) -> list[tuple[int, str]]:
    runner_aliases: dict[str, str] = {}
    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                if imported.name in _RUNNER_MODULES and imported.asname is not None:
                    runner_aliases[imported.asname] = imported.name
        elif isinstance(node, ast.ImportFrom):
            if node.module in _RUNNER_MODULES:
                for imported in node.names:
                    if imported.name.startswith("_"):
                        violations.append(
                            (
                                node.lineno,
                                f"imports {imported.name} from {node.module}",
                            )
                        )
            elif node.module == "sendai_pipeline":
                for imported in node.names:
                    module_name = f"sendai_pipeline.{imported.name}"
                    if module_name in _RUNNER_MODULES:
                        runner_aliases[imported.asname or imported.name] = module_name

    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute) or not node.attr.startswith("_"):
            continue
        owner = _qualified_name(node.value)
        module_name = runner_aliases.get(owner or "", owner)
        if module_name in _RUNNER_MODULES:
            violations.append((node.lineno, f"accesses {node.attr} on {module_name}"))

    return violations


def test_private_runner_dependency_check_catches_supported_import_forms() -> None:
    sources = [
        "from sendai_pipeline.run_flow import _process_send_window",
        (
            "from sendai_pipeline import run_flow as runner\n"
            "runner._process_send_window()"
        ),
        (
            "import sendai_pipeline.run_direction as runner\n"
            "runner._process_send_window()"
        ),
        (
            "import sendai_pipeline.run_direction\n"
            "sendai_pipeline.run_direction._process_send_window()"
        ),
    ]

    for source in sources:
        assert _private_runner_dependencies(ast.parse(source))


def test_private_runner_dependency_check_allows_public_runner_imports() -> None:
    source = (
        "from sendai_pipeline.run_flow import replay_flow_window\n"
        "from sendai_pipeline import run_direction\n"
        "run_direction.publish_direction_window()"
    )

    assert _private_runner_dependencies(ast.parse(source)) == []


def test_scripts_do_not_import_private_runner_names() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    violations: list[str] = []

    for path in sorted((repo_root / "scripts").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative_path = path.relative_to(repo_root)
        for line, detail in _private_runner_dependencies(tree):
            violations.append(f"{relative_path}:{line} {detail}")

    assert violations == []
