import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = sorted((ROOT / "src" / "notebooks").glob("*.ipynb"))
BANNED = [
    "akrishn_fe_dsa",
    "azure-aiquery",
    "adb-7405609619727450",
    "/Users/archana",
    "archana.krishnamurthy",
    "Vizient",
    "vizient",
    "gpo_febar_repro",
]


def load_notebook(path):
    return json.loads(path.read_text(encoding="utf-8"))


def source(cell):
    value = cell.get("source", [])
    return value if isinstance(value, str) else "".join(value)


class NotebookPolicyTests(unittest.TestCase):
    def test_ipynb_has_markdown_before_code(self):
        self.assertTrue(NOTEBOOKS)
        for path in NOTEBOOKS:
            cells = load_notebook(path)["cells"]
            self.assertEqual(cells[0]["cell_type"], "markdown", path.name)
            for index, cell in enumerate(cells):
                if cell["cell_type"] == "code":
                    self.assertEqual(cells[index - 1]["cell_type"], "markdown")
                    self.assertTrue(source(cells[index - 1]).strip())
                    self.assertNotIn("spark.conf.set", source(cell))


class RepositoryContractTests(unittest.TestCase):
    def test_bundle_assets_exist(self):
        for rel in [
            "databricks.yml",
            "SETUP.md",
            "README.md",
            "resources/gpo_job.yml",
            "resources/gpo_pipeline.yml",
            "resources/gpo_app.yml",
            "src/notebooks/00_generate_synthetic_data.py",
            "src/tests/validate_build.py",
            "docs/diagrams/gpo_spend_intelligence.svg",
        ]:
            self.assertTrue((ROOT / rel).is_file(), rel)

    def test_portable_files_have_no_personal_or_customer_values(self):
        paths = [
            ROOT / "README.md",
            ROOT / "SETUP.md",
            ROOT / "databricks.yml",
            *NOTEBOOKS,
            *sorted((ROOT / "src").rglob("*.py")),
            *sorted((ROOT / "resources").glob("*")),
        ]
        for path in paths:
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8")
            for value in BANNED:
                self.assertNotIn(value, text, f"{value} in {path.relative_to(ROOT)}")

    def test_readme_and_setup_have_no_em_dash(self):
        for name in ("README.md", "SETUP.md", "databricks.yml"):
            text = (ROOT / name).read_text(encoding="utf-8")
            self.assertNotIn("\u2014", text, name)
            self.assertNotIn("\u2013", text, name)

    def test_readme_does_not_reference_missing_assets(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("synthetic", readme.lower())
        self.assertNotIn("evidence/", readme)
        for relative in re.findall(r"\[[^\]]+\]\(([^)]+)\)", readme):
            if "://" not in relative and not relative.startswith("#"):
                self.assertTrue((ROOT / relative).exists(), relative)

    def test_required_deploy_vars_have_no_workspace_defaults(self):
        yml = (ROOT / "databricks.yml").read_text(encoding="utf-8")
        self.assertIn("warehouse_id:", yml)
        self.assertIn("genie_space_id:", yml)
        self.assertNotIn("default: 106bb", yml)
        self.assertNotIn("01f1b6fc", yml)


if __name__ == "__main__":
    unittest.main()
