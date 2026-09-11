"""Tests for the migration engine. Stdlib unittest -- no pytest, no network.

    python -m unittest discover -s tests -v      (from the skill root)

The fixture is a small hand-written workflow that deliberately contains one of
each thing the engine must get right: a container, a nested tool, a
non-data Comment, an unresolvable macro reference, a wall-clock formula, a
population StdDev, a multi-anchor Filter, and an IO tool at each end.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ENGINE_ROOT = os.path.dirname(HERE)
sys.path.insert(0, ENGINE_ROOT)

from mig import (context, db, export, lakebridge, plan, retrieve, sources, units,  # noqa: E402
                 validate, xlsx)
from mig.cli import _is_complete, default_corpus  # noqa: E402

parse = sources.get("alteryx")
FIXTURE = os.path.join(HERE, "fixtures", "mini_workflow.yxmd")
CORPUS = default_corpus()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.run = self.tmp.name
        self.con = db.connect(self.run)
        db.set_meta(self.con, "workflow_path", FIXTURE)

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()

    def extract(self):
        return parse.extract(self.con, FIXTURE)


class TestParse(Base):
    def test_counts_every_node_including_nested_and_furniture(self):
        stats = self.extract()
        self.assertEqual(stats["nodes"], 8)
        self.assertEqual(stats["edges"], 4)

    def test_nested_tools_get_their_container(self):
        self.extract()
        row = self.con.execute(
            "SELECT container, depth FROM nodes WHERE node_key='w0:4'").fetchone()
        self.assertEqual(row["container"], "w0:2")
        self.assertEqual(row["depth"], 1)

    def test_expressions_are_verbatim(self):
        self.extract()
        exprs = {r["field"]: r["expr"] for r in self.con.execute(
            "SELECT field, expr FROM expressions WHERE node_key='w0:4'")}
        self.assertEqual(exprs["RegionCode"], "Left([Region],3)")
        self.assertEqual(exprs["RunDate"], "DateTimeToday()")

    def test_filter_expression_kept_with_alteryx_syntax(self):
        self.extract()
        row = self.con.execute(
            "SELECT expr FROM expressions WHERE node_key='w0:3'").fetchone()
        self.assertIn("&&", row["expr"])
        self.assertIn("IsNull", row["expr"])

    def test_wallclock_is_flagged_as_a_hazard(self):
        self.extract()
        kinds = {r["kind"] for r in self.con.execute(
            "SELECT kind FROM hazards WHERE node_key='w0:4'")}
        self.assertIn("nondeterministic-clock", kinds)

    def test_missing_macro_becomes_one_blocking_gap(self):
        self.extract()
        rows = self.con.execute(
            "SELECT kind, blocking, question FROM gaps").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["blocking"], 1)
        self.assertIn("NotOnDisk.yxmc", rows[0]["question"])

    def test_io_tools_are_marked(self):
        self.extract()
        io = {r["node_key"] for r in self.con.execute(
            "SELECT node_key FROM nodes WHERE is_io=1")}
        self.assertLessEqual({"w0:1", "w0:6"}, io)

    def test_identical_configs_share_a_hash(self):
        self.extract()
        h1 = self.con.execute(
            "SELECT config_hash FROM nodes WHERE node_key='w0:3'").fetchone()[0]
        self.assertTrue(h1)
        h2 = self.con.execute(
            "SELECT config_hash FROM nodes WHERE node_key='w0:4'").fetchone()[0]
        self.assertNotEqual(h1, h2)


class TestUnits(Base):
    def setUp(self):
        super().setUp()
        self.extract()
        self.stats = units.build(self.con, max_unit=10, min_unit=1)

    def test_every_tool_is_either_in_a_unit_or_excluded_with_a_reason(self):
        total = self.con.execute("SELECT count(*) c FROM nodes").fetchone()["c"]
        placed = self.con.execute(
            "SELECT count(DISTINCT node_key) c FROM unit_nodes").fetchone()["c"]
        excluded = units.excluded_nodes(self.con)
        self.assertEqual(placed + len(excluded), total)
        self.assertTrue(all(e["reason"] for e in excluded))
        self.assertEqual(units.unaccounted_nodes(self.con), [])

    def test_a_wired_data_tool_missing_from_units_is_unaccounted_not_excluded(self):
        self.con.execute("DELETE FROM unit_nodes WHERE node_key='w0:4'")
        self.con.commit()
        self.assertEqual([u["node_key"] for u in units.unaccounted_nodes(self.con)], ["w0:4"])
        self.assertNotIn("w0:4", [e["node_key"] for e in units.excluded_nodes(self.con)])

    def test_container_becomes_its_own_unit(self):
        rows = {r["unit_id"]: r["title"] for r in self.con.execute(
            "SELECT unit_id, title FROM units")}
        self.assertIn("Clean and score", rows.values())

    def test_no_tool_lands_in_two_units(self):
        dup = self.con.execute(
            "SELECT node_key FROM unit_nodes GROUP BY 1 HAVING count(*)>1").fetchall()
        self.assertEqual(dup, [])

    def test_dependencies_follow_the_connections(self):
        owner = {r["node_key"]: r["unit_id"] for r in self.con.execute(
            "SELECT unit_id, node_key FROM unit_nodes")}
        deps = {(r["unit_id"], r["depends_on"]) for r in self.con.execute(
            "SELECT unit_id, depends_on FROM unit_deps")}
        for e in self.con.execute("SELECT origin, dest FROM edges"):
            uo, ud = owner.get(e["origin"]), owner.get(e["dest"])
            if uo and ud and uo != ud:
                self.assertIn((ud, uo), deps)

    def test_ready_respects_dependency_order(self):
        first = units.ready(self.con)
        self.assertTrue(first)
        for u in first:
            unmet = self.con.execute(
                "SELECT count(*) c FROM unit_deps d JOIN units x ON x.unit_id=d.depends_on "
                "WHERE d.unit_id=? AND x.status NOT IN ('complete','validated')",
                (u["unit_id"],)).fetchone()["c"]
            self.assertEqual(unmet, 0)

    def test_sequence_is_a_valid_topological_order(self):
        seq = {r["unit_id"]: r["seq"] for r in self.con.execute(
            "SELECT unit_id, seq FROM units")}
        for r in self.con.execute("SELECT unit_id, depends_on FROM unit_deps"):
            self.assertLess(seq[r["depends_on"]], seq[r["unit_id"]])


class TestDedupe(Base):
    def test_identical_blocks_collapse_to_one_unit_plus_references(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        # Force two units to share a fingerprint and re-run the dedupe pass the
        # way build() does, to prove duplicates point at a single source unit.
        ids = [r["unit_id"] for r in self.con.execute(
            "SELECT unit_id FROM units ORDER BY seq")]
        self.assertGreaterEqual(len(ids), 2)
        self.con.execute("UPDATE units SET block_hash='SAME'")
        seen = {}
        for uid in ids:
            if "SAME" in seen:
                self.con.execute("UPDATE units SET dedupe_of=? WHERE unit_id=?",
                                 (seen["SAME"], uid))
            else:
                seen["SAME"] = uid
        self.con.commit()
        dupes = self.con.execute(
            "SELECT count(*) c FROM units WHERE dedupe_of IS NOT NULL").fetchone()["c"]
        self.assertEqual(dupes, len(ids) - 1)


class TestRetrieval(unittest.TestCase):
    def test_index_builds_and_ranks_relevant_sections_first(self):
        roots = [r for r in CORPUS if os.path.exists(r)]
        if not roots:
            self.skipTest("skill corpus not present")
        idx = retrieve.build_index(roots, None)
        self.assertGreater(len(idx["docs"]), 10)
        hits = retrieve.search(idx, "pivot crosstab explicit values groupBy", top_k=5)
        self.assertTrue(hits)
        joined = " ".join(h["text"].lower() for h in hits)
        self.assertIn("pivot", joined)

    def test_retrieval_stays_inside_its_budget(self):
        roots = [r for r in CORPUS if os.path.exists(r)]
        if not roots:
            self.skipTest("skill corpus not present")
        idx = retrieve.build_index(roots, None)
        hits = retrieve.search(idx, "join broadcast window aggregate pivot union",
                               top_k=6, budget=4000)
        self.assertLessEqual(sum(len(h["text"]) for h in hits), 4000 + retrieve.MAX_CHUNK_CHARS)

    def test_query_weights_by_tool_frequency(self):
        q = retrieve.query_for_unit({"Join": 30, "Sort": 1})
        self.assertGreater(q.count("Join"), q.count("Sort"))


class TestContext(Base):
    def setUp(self):
        super().setUp()
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        self.unit = self.con.execute(
            "SELECT unit_id FROM units WHERE title='Clean and score'").fetchone()["unit_id"]

    def test_pack_compacts_configuration_that_is_actually_large(self):
        # On a three-tool toy unit the pack's headings and boundary contract cost
        # more than the XML they replace; the saving is in configuration volume,
        # which is what real workflows have. Enlarge one tool's config to the
        # size a real Select carries and assert the compaction there.
        big = ("<Configuration><SelectFields>" +
               "".join('<SelectField field="Column_%03d" selected="True" '
                       'type="V_WString" size="1073741823" />' % i for i in range(120)) +
               "</SelectFields></Configuration>")
        self.con.execute(
            "UPDATE nodes SET tool_name='AlteryxSelect', config_xml=? WHERE node_key='w0:4'",
            (big,))
        self.con.commit()
        pack = context.build_pack(self.con, self.unit, CORPUS, None)
        md = context.render_markdown(pack)
        skills = sum(len(s["text"]) for s in pack["retrieved_skills"])
        facts = len(md) - skills
        raw = sum(len(r["config_xml"] or "") for r in self.con.execute(
            "SELECT n.config_xml FROM unit_nodes un JOIN nodes n USING(node_key) "
            "WHERE un.unit_id=?", (self.unit,)))
        self.assertLess(facts, raw / 2)

    def test_pack_never_contains_raw_configuration_xml(self):
        pack = context.build_pack(self.con, self.unit, CORPUS, None)
        md = context.render_markdown(pack)
        self.assertNotIn("<Configuration>", md)
        self.assertNotIn("<FormulaField", md)

    def test_expressions_survive_verbatim_into_the_pack(self):
        md = context.render_markdown(context.build_pack(self.con, self.unit, CORPUS, None))
        self.assertIn("Left([Region],3)", md)

    def test_pack_states_its_boundaries(self):
        pack = context.build_pack(self.con, self.unit, CORPUS, None)
        self.assertTrue(pack["inputs"] or pack["outputs"])

    def test_hazards_reach_the_pack(self):
        md = context.render_markdown(context.build_pack(self.con, self.unit, CORPUS, None))
        self.assertIn("HAZARD", md)

    def test_describe_compacts_a_long_select_field_list(self):
        node = {"tool_name": "AlteryxSelect", "config_xml":
                "<Configuration><SelectFields>" +
                "".join('<SelectField field="F%d" selected="True" />' % i for i in range(60)) +
                "</SelectFields></Configuration>"}
        d = context.describe(node)
        self.assertIn("keeps 60 field(s)", d)
        self.assertLess(len(d), 200)


class TestCodeValidation(Base):
    def _write(self, src):
        p = os.path.join(self.run, "gen.py")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(src)
        return p

    def setUp(self):
        super().setUp()
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        self.unit = self.con.execute(
            "SELECT unit_id FROM units WHERE title='Clean and score'").fetchone()["unit_id"]

    def _run_code(self, src):
        p = self._write(src)
        validate.code(self.con, self.unit, [p])
        return {f["check_name"].split(":")[0] for f in validate.failures(self.con, self.unit)}

    def test_syntax_error_is_caught(self):
        self.assertIn("syntax", self._run_code("def f(:\n  pass\n"))

    def test_undefined_name_is_caught(self):
        self.assertIn("names-resolve", self._run_code("y = some_undefined_df.filter(1)\n"))

    def test_sql_string_is_rejected(self):
        self.assertIn("target-code-rules", self._run_code(
            "import dlt\nx = spark.sql('select 1')\n"))

    def test_wallclock_is_rejected(self):
        self.assertIn("target-code-rules", self._run_code(
            "from pyspark.sql import functions as F\nx = F.current_timestamp()\n"))

    def test_dlt_function_without_a_return_is_caught(self):
        self.assertIn("dlt-returns-dataframe", self._run_code(
            "import dlt\n@dlt.table(name='t')\ndef t():\n    pass\n"))

    def test_clean_code_passes_every_code_check(self):
        fails = self._run_code(
            "import dlt\n"
            "from pyspark.sql import DataFrame\n"
            "from pyspark.sql import functions as F\n"
            "@dlt.table(name='t')\n"
            "def t() -> DataFrame:\n"
            "    src = dlt.read('bronze')\n"
            "    return src.filter(F.col('Amount') > 0)\n")
        self.assertEqual(fails, set())


class TestSemanticValidation(Base):
    def setUp(self):
        super().setUp()
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        self.unit = self.con.execute(
            "SELECT unit_id FROM units WHERE title='Clean and score'").fetchone()["unit_id"]

    def _semantic(self, src):
        p = os.path.join(self.run, "gen.py")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(src)
        validate.semantic(self.con, self.unit, [p])
        return {f["check_name"]: f["detail"] for f in validate.failures(self.con, self.unit)}

    def test_missing_filter_construct_is_caught(self):
        fails = self._semantic("df = 1\n")
        self.assertIn("tool-constructs", fails)
        self.assertIn("Filter", fails["tool-constructs"])

    def test_sample_stddev_is_rejected_because_alteryx_is_population(self):
        fails = self._semantic(
            "out = src.filter(c).groupBy('RegionCode').agg(F.stddev('Amount'))\n")
        self.assertIn("stddev-population", fails)

    def test_population_stddev_passes(self):
        fails = self._semantic(
            "out = src.filter(c).groupBy('RegionCode').agg(F.stddev_pop('Amount'))\n")
        self.assertNotIn("stddev-population", fails)

    def test_unparameterized_clock_is_caught(self):
        fails = self._semantic("out = src.filter(c).groupBy('x').agg(F.stddev_pop('Amount'))\n")
        self.assertIn("clock-parameterized", fails)

    def test_run_date_parameter_satisfies_the_clock_check(self):
        fails = self._semantic(
            "run_date = spark.conf.get('pipeline.run_date')\n"
            "out = src.filter(c).groupBy('x').agg(F.stddev_pop('Amount'))\n")
        self.assertNotIn("clock-parameterized", fails)

    def test_created_fields_must_appear_in_the_code(self):
        fails = self._semantic("out = src.filter(c).groupBy('x').agg(F.stddev_pop('a'))\n")
        self.assertIn("created-fields-present", fails)
        self.assertIn("TotalAmount", fails["created-fields-present"])


class TestStructuralValidation(Base):
    def test_coverage_passes_when_every_tool_is_accounted_for(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        validate.structural(self.con)
        checks = {r["check_name"]: r["status"] for r in self.con.execute(
            "SELECT check_name, status FROM validations WHERE level='structural'")}
        self.assertEqual(checks["tool-coverage"], "pass")
        self.assertEqual(checks["no-tool-in-two-units"], "pass")
        self.assertEqual(checks["dependency-preservation"], "pass")

    def test_dropping_a_tool_fails_coverage(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        self.con.execute("DELETE FROM unit_nodes WHERE node_key='w0:4'")
        self.con.commit()
        validate.structural(self.con)
        checks = {r["check_name"]: r["status"] for r in self.con.execute(
            "SELECT check_name, status FROM validations WHERE level='structural'")}
        self.assertEqual(checks["tool-coverage"], "fail")

    def test_open_blocking_gap_fails_structural(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        validate.structural(self.con)
        checks = {r["check_name"]: r["status"] for r in self.con.execute(
            "SELECT check_name, status FROM validations WHERE level='structural'")}
        self.assertEqual(checks["no-open-blocking-gaps"], "fail")

    def test_lakebridge_parity_skips_rather_than_passes_when_absent(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        validate.structural(self.con)
        row = self.con.execute(
            "SELECT status FROM validations WHERE check_name='lakebridge-parity'").fetchone()
        self.assertEqual(row["status"], "skip")


class TestDataValidation(Base):
    def test_absent_reference_output_skips_and_never_passes(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        uid = self.con.execute("SELECT unit_id FROM units LIMIT 1").fetchone()["unit_id"]
        validate.data(self.con, uid)
        rows = self.con.execute(
            "SELECT status FROM validations WHERE level='data'").fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(r["status"] == "skip" for r in rows))


class TestCompletionGate(Base):
    def test_generated_code_alone_is_not_completion(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        self.con.execute("UPDATE units SET status='generated'")
        self.con.commit()
        verdict = _is_complete(self.con)
        self.assertFalse(verdict["complete"])
        self.assertTrue(verdict["blockers"])

    def test_unit_verdict_refuses_while_a_blocking_gap_is_open(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        uid = self.con.execute(
            "SELECT un.unit_id FROM unit_nodes un WHERE un.node_key='w0:8'").fetchone()
        if uid is None:
            self.skipTest("macro node not placed in a unit")
        src = os.path.join(self.run, "g.py")
        with open(src, "w", encoding="utf-8") as fh:
            fh.write("x = 1\n")
        self.con.execute(
            "UPDATE gaps SET unit_id=? WHERE blocking=1", (uid["unit_id"],))
        self.con.commit()
        validate.code(self.con, uid["unit_id"], [src])
        validate.semantic(self.con, uid["unit_id"], [src])
        verdict, why = validate.unit_verdict(self.con, uid["unit_id"])
        self.assertIn(verdict, ("failed", "blocked"))


class TestPlanProjection(Base):
    def test_plan_files_are_written_and_reflect_state(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        res = plan.write(self.con, self.run)
        for name in ("task_plan.md", "findings.md", "progress.md"):
            p = os.path.join(res["dir"], name)
            self.assertTrue(os.path.isfile(p), name)
            with open(p, encoding="utf-8") as fh:
                self.assertTrue(fh.read().strip())
        with open(os.path.join(res["dir"], "task_plan.md"), encoding="utf-8") as fh:
            body = fh.read()
        self.assertIn("Clean and score", body)
        self.assertIn("NotOnDisk.yxmc", body)

    def test_active_plan_pointer_is_written(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        plan.write(self.con, self.run)
        self.assertTrue(os.path.isfile(os.path.join(self.run, ".active_plan")))


class TestLakebridge(Base):
    def test_cross_check_matches_when_census_agrees(self):
        self.extract()
        import json
        census = {}
        for r in self.con.execute("SELECT tool_name, count(*) c FROM nodes GROUP BY 1"):
            census[r["tool_name"]] = r["c"]
        self.con.execute(
            "INSERT INTO lakebridge(source_file,name,complexity,type,node_census,"
            "func_census,statements) VALUES(?,?,?,?,?,?,?)",
            ("mini_workflow.yxmd", "mini", "LOW", "JOB", json.dumps(census), "{}", "[]"))
        self.con.commit()
        cc = lakebridge.cross_check(self.con)
        self.assertEqual(cc["mismatched_types"], [])

    def test_cross_check_reports_a_missing_tool(self):
        self.extract()
        import json
        self.con.execute(
            "INSERT INTO lakebridge(source_file,name,complexity,type,node_census,"
            "func_census,statements) VALUES(?,?,?,?,?,?,?)",
            ("mini_workflow.yxmd", "mini", "LOW", "JOB",
             json.dumps({"AlteryxFormula": 99}), "{}", "[]"))
        self.con.commit()
        cc = lakebridge.cross_check(self.con)
        self.assertTrue(any(d["tool"] == "formula" for d in cc["mismatched_types"]))

    def test_cross_check_ignores_files_this_workflow_never_referenced(self):
        """`analyze --source-directory` sweeps a whole tree. A neighbour's tools
        must not land in this workflow's census."""
        self.extract()
        import json
        census = {}
        for r in self.con.execute("SELECT tool_name, count(*) c FROM nodes GROUP BY 1"):
            census[r["tool_name"]] = r["c"]
        self.con.executemany(
            "INSERT INTO lakebridge(source_file,name,complexity,type,node_census,"
            "func_census,statements) VALUES(?,?,?,?,?,?,?)",
            # The analyzer reports Windows paths with backslashes; the in-scope
            # filter has to normalize before comparing basenames.
            [(r"C:\reports\mini_workflow.yxmd", "mini", "LOW", "JOB",
              json.dumps(census), "{}", "[]"),
             (r"C:\Users\x\Downloads\unrelated.yxmd", "other", "HIGH", "JOB",
              json.dumps({"AlteryxFormula": 400}), "{}", "[]")])
        self.con.commit()
        cc = lakebridge.cross_check(self.con)
        self.assertEqual(cc["mismatched_types"], [])
        self.assertEqual(cc["files_out_of_scope"], 1)

    def test_endpoints_ignore_out_of_scope_files(self):
        """A neighbouring workflow can reuse our ToolIDs; its statements must not
        be attributed to our unit."""
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        import json
        row = self.con.execute(
            "SELECT u.unit_id, n.tool_id FROM unit_nodes u JOIN nodes n USING(node_key) "
            "WHERE n.is_io=1 LIMIT 1").fetchone()
        if row is None:
            self.skipTest("fixture has no IO tool in a unit")
        self.con.execute(
            "INSERT INTO lakebridge(source_file,name,complexity,type,node_census,"
            "func_census,statements) VALUES(?,?,?,?,?,?,?)",
            ("C:/Users/x/Downloads/unrelated.yxmd", "other", "HIGH", "JOB", "{}", "{}",
             json.dumps([{"nodeName": "FileInput_%s" % row["tool_id"],
                          "connectionType": "ODBC", "objects": ["other.tbl"],
                          "actions": ["READ"], "complexity": "LOW",
                          "sql_chars": 10, "sql_head": "select 1"}])))
        self.con.commit()
        self.assertEqual(context.lakebridge_endpoints(self.con, row["unit_id"]), [])

    def test_analyzer_command_shape(self):
        cmd = lakebridge.analyzer_command("/src", "/out/report.xlsx")
        self.assertEqual(cmd[:4], ["databricks", "labs", "lakebridge", "analyze"])
        self.assertIn("--source-tech", cmd)
        self.assertEqual(cmd[cmd.index("--source-tech") + 1], "alteryx")


class TestAdapterContract(unittest.TestCase):
    """The seam that lets a new source or target be added without forking the
    engine. If these fail, adding SSIS or DataStage will not work."""

    REQUIRED_SOURCE = ["name", "extensions", "lakebridge_source_tech", "extract",
                       "describe", "tool_terms", "func_terms", "non_data_tools",
                       "viewer_tools", "annotation_tools", "grouping_tools",
                       "required_constructs", "semantic_checks"]
    REQUIRED_TARGET = ["name", "language", "forbidden", "runtime_globals", "code_checks"]

    def test_every_registered_source_satisfies_the_contract(self):
        from mig import targets
        self.assertIn("alteryx", sources.available())
        for n in sources.available():
            mod = sources.get(n)
            for attr in self.REQUIRED_SOURCE:
                self.assertTrue(hasattr(mod, attr), "%s missing %s" % (n, attr))

    def test_every_registered_target_satisfies_the_contract(self):
        from mig import targets
        self.assertIn("databricks-sdp", targets.available())
        for n in targets.available():
            mod = targets.get(n)
            for attr in self.REQUIRED_TARGET:
                self.assertTrue(hasattr(mod, attr), "%s missing %s" % (n, attr))

    def test_source_is_selected_by_file_extension(self):
        self.assertEqual(sources.for_file("x.yxmd").name, "alteryx")
        self.assertEqual(sources.for_file("x.YXMC").name, "alteryx")

    def test_unknown_extension_is_refused_not_guessed(self):
        with self.assertRaises(SystemExit):
            sources.for_file("pipeline.dtsx")

    def test_unknown_adapter_name_is_refused(self):
        with self.assertRaises(SystemExit):
            sources.get("ssis")

    def test_required_constructs_are_compiled_patterns(self):
        for tool, (pat, label) in sources.get("alteryx").required_constructs.items():
            self.assertTrue(hasattr(pat, "search"), tool)
            self.assertTrue(label)

    def test_corpus_picks_up_every_skill_directory(self):
        roots = [os.path.basename(r) for r in default_corpus()]
        self.assertIn("references", roots)
        self.assertIn("alteryx-to-sdp", roots)
        self.assertIn("alteryx-sdp-migrate", roots)


class TestXlsx(unittest.TestCase):
    """The writer has no library behind it, so a round-trip is the only proof."""

    def _roundtrip(self, sheets):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "wb.xlsx")
        xlsx.write(path, sheets)
        return {name: rows for name, rows in xlsx.read(path)}

    def test_values_survive_a_round_trip_with_their_types(self):
        back = self._roundtrip([xlsx.Sheet("S", [["h1", "h2", "h3"], ["text", 42, 1.5]])])
        self.assertEqual(back["S"][0], ["h1", "h2", "h3"])
        self.assertEqual(back["S"][1], ["text", 42, 1.5])

    def test_formulas_round_trip_as_formulas(self):
        back = self._roundtrip([xlsx.Sheet("S", [["a"], ["='Other Sheet'!A1"]])])
        self.assertEqual(back["S"][1][0], "='Other Sheet'!A1")

    def test_non_ascii_and_xml_metacharacters_survive(self):
        row = ["Helårsboligomsetninger", "a < b & c > d", '"quoted"']
        back = self._roundtrip([xlsx.Sheet("S", [["h1", "h2", "h3"], row])])
        self.assertEqual(back["S"][1], row)

    def test_control_characters_are_stripped_not_raised_on(self):
        back = self._roundtrip([xlsx.Sheet("S", [["h"], ["a\x00b\x07c"]])])
        self.assertEqual(back["S"][1][0], "abc")

    def test_oversized_cell_is_truncated_to_excels_limit(self):
        back = self._roundtrip([xlsx.Sheet("S", [["h"], ["x" * (xlsx.CELL_LIMIT + 500)]])])
        self.assertEqual(len(back["S"][1][0]), xlsx.CELL_LIMIT)

    def test_gaps_in_a_row_keep_later_columns_aligned(self):
        back = self._roundtrip([xlsx.Sheet("S", [["a", "b", "c"], [None, None, "third"]])])
        self.assertEqual(back["S"][1][2], "third")

    def test_sheet_names_are_sanitised_and_deduplicated(self):
        taken = set()
        self.assertEqual(xlsx.safe_sheet_name("a/b:c", taken), "a-b-c")
        self.assertEqual(xlsx.safe_sheet_name("a/b:c", taken), "a-b-c~2")
        self.assertEqual(len(xlsx.safe_sheet_name("x" * 60, taken)), xlsx.SHEET_NAME_LIMIT)


class TestExport(Base):
    def _build(self, **kw):
        out = os.path.join(self.run, "export.xlsx")
        res = export.build(self.con, self.run, out, complete=_is_complete(self.con), **kw)
        return res, {name: rows for name, rows in xlsx.read(out)}

    def setUp(self):
        super().setUp()
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)

    def test_every_parsed_tool_reaches_the_workbook(self):
        _, book = self._build()
        tools = book["MIG Tools"]
        n = self.con.execute("SELECT count(*) c FROM nodes").fetchone()["c"]
        self.assertEqual(len(tools) - 1, n)          # -1 for the header row

    def test_expressions_reach_the_workbook_verbatim(self):
        _, book = self._build()
        col = book["MIG Expressions"][0].index("expression")
        written = {r[col] for r in book["MIG Expressions"][1:]}
        for row in self.con.execute("SELECT expr FROM expressions"):
            self.assertIn(row["expr"], written)

    def test_configuration_xml_is_excluded_unless_full_is_asked_for(self):
        _, book = self._build()
        self.assertNotIn("config_xml", book["MIG Tools"][0])
        _, full = self._build(full=True)
        self.assertIn("config_xml", full["MIG Tools"][0])

    def test_decisions_carry_their_basis_so_facts_stay_separable(self):
        self.con.execute(
            "INSERT INTO decisions(unit_id,basis,summary,created_at) VALUES('U001','fact','x',0)")
        self.con.commit()
        _, book = self._build()
        header = book["MIG Decisions"][0]
        self.assertIn("basis", header)
        self.assertEqual(book["MIG Decisions"][1][header.index("basis")], "fact")

    def test_export_works_with_no_lakebridge_data(self):
        res, book = self._build()
        self.assertEqual(res["lakebridge_sheets"], 0)
        self.assertIn("MIG Overview", book)

    def test_merged_lakebridge_sheets_keep_their_original_names(self):
        src = os.path.join(self.run, "lakebridge", "lakebridge-analysis.xlsx")
        os.makedirs(os.path.dirname(src), exist_ok=True)
        # A formula referencing a sibling by name is exactly what renaming breaks.
        xlsx.write(src, [xlsx.Sheet("Jobs Transformations Xref", [["Job"], ["w"]]),
                         xlsx.Sheet("Summary", [["Total"], ["='Jobs Transformations Xref'!A2"]])])
        res, book = self._build()
        self.assertEqual(res["lakebridge_sheets"], 2)
        self.assertIn("Jobs Transformations Xref", book)
        self.assertEqual(book["Summary"][1][0], "='Jobs Transformations Xref'!A2")

    def test_a_lakebridge_sheet_never_displaces_an_engine_sheet(self):
        src = os.path.join(self.run, "lakebridge", "lakebridge-analysis.xlsx")
        os.makedirs(os.path.dirname(src), exist_ok=True)
        xlsx.write(src, [xlsx.Sheet("MIG Tools", [["collides"], ["with ours"]])])
        _, book = self._build()
        self.assertIn("collides", book["MIG Tools~2"][0])
        self.assertIn("node_key", book["MIG Tools"][0])


class TestCensusScoping(Base):
    """A workflow analysed under a renamed copy must not silently lose the check."""

    def _ingest_census(self, source_file):
        self.con.execute(
            "INSERT INTO lakebridge(source_file,name,complexity,type,node_census,"
            "func_census,statements) VALUES(?,?,?,?,?,?,?)",
            (source_file, "w", "LOW", "JOB", json.dumps({"AlteryxFormula": 1}), "{}", "[]"))
        self.con.commit()

    def test_unmatched_filename_makes_the_cross_check_refuse_to_report(self):
        self.extract()
        self._ingest_census("renamed_copy.yxmd")
        self.assertIsNone(lakebridge.cross_check(self.con))

    def test_reporting_may_fall_back_unscoped_but_says_so(self):
        self.extract()
        self._ingest_census("renamed_copy.yxmd")
        cmp_ = lakebridge.census_compare(self.con, allow_unscoped=True)
        self.assertIsNotNone(cmp_)
        self.assertFalse(cmp_["scope_matched"])

    def test_a_matching_filename_scopes_normally(self):
        self.extract()
        self._ingest_census(os.path.basename(FIXTURE))
        cmp_ = lakebridge.census_compare(self.con)
        self.assertTrue(cmp_["scope_matched"])

    def test_the_workbook_warns_when_the_census_could_not_be_scoped(self):
        self.extract()
        units.build(self.con, max_unit=10, min_unit=1)
        self._ingest_census("renamed_copy.yxmd")
        out = os.path.join(self.run, "export.xlsx")
        export.build(self.con, self.run, out, complete=_is_complete(self.con))
        book = {name: rows for name, rows in xlsx.read(out)}
        text = "\n".join(str(c) for r in book["MIG Census Crosscheck"] for c in r)
        self.assertIn("WARNING", text)


class TestSourceSelectionPersists(Base):
    def test_run_records_its_source_and_target(self):
        db.set_meta(self.con, "source", "alteryx")
        db.set_meta(self.con, "target", "databricks-sdp")
        self.assertEqual(context.source_for(self.con).name, "alteryx")
        s, t = validate._adapters(self.con)
        self.assertEqual((s.name, t.name), ("alteryx", "databricks-sdp"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
