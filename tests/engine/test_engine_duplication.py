from __future__ import annotations

import ast
from pathlib import Path

def test_pipelines_engine_is_thin_wrapper():
    """
    Ensure that project/pipelines/engine/ files are strictly thin wrappers 
    around project/engine/ to prevent logic divergence and duplication.
    """
    project_root = Path(__file__).resolve().parents[2] / "project"
    pipelines_engine_dir = project_root / "pipelines" / "engine"
    
    expected_wrappers = ["execution_model.py", "pnl.py", "risk_allocator.py", "runner.py"]
    
    for filename in expected_wrappers:
        filepath = pipelines_engine_dir / filename
        assert filepath.exists(), f"Wrapper {filename} missing"
        
        content = filepath.read_text(encoding="utf-8")
        tree = ast.parse(content)
        
        # Strictly ensure no complex logic (only Docstrings and ImportFrom allowed)
        for node in tree.body:
            assert isinstance(node, (ast.ImportFrom, ast.Expr)), \
                f"{filename} contains actual logic rather than being a wrapper!"
            
        imports = [n for n in tree.body if isinstance(n, ast.ImportFrom)]
        assert len(imports) >= 1, f"Missing import in {filename}"
        
        found_redirect = False
        module_name = filename.replace(".py", "")
        for imp in imports:
            # We enforce 'from engine.module_name import *'
            if imp.module == f"engine.{module_name}" and any(alias.name == "*" for alias in imp.names):
                found_redirect = True
                break
                
        assert found_redirect, f"{filename} must strictly re-export via 'from engine.{module_name} import *'"
