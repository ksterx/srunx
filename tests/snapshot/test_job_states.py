"""Tests for srunx.snapshot.job_states."""

from __future__ import annotations

import re

import pytest

from srunx.snapshot.job_states import (
    SACCT_CHUNK,
    SQUEUE_SEP,
    TERMINAL_STATES,
    build_cluster_command,
    build_queue_command,
    build_scontrol_command,
    parse_queue_output,
    parse_scontrol_output,
    query_job_cluster,
    query_job_states,
)


def _nonce(cmd: str) -> str:
    m = re.search(r"__SRUNX_([0-9a-f]+)_CLUSTERQ__", cmd)
    assert m
    return m.group(1)


class FakeExecute:
    """Answers the two cluster lookups: slurmctld (squeue) and accounting (sacct)."""

    def __init__(
        self,
        squeue: str = "",
        sacct: str = "",
        *,
        squeue_rc: int = 0,
        sacct_rc: int = 0,
        code: int = 0,
        noise: str = "",
    ) -> None:
        self.squeue, self.sacct = squeue, sacct
        self.squeue_rc, self.sacct_rc = squeue_rc, sacct_rc
        self.code, self.noise = code, noise
        self.commands: list[str] = []

    def __call__(self, cmd: str) -> tuple[str, str, int]:
        self.commands.append(cmd)
        n = _nonce(cmd)
        out = (
            f"{self.noise}__SRUNX_{n}_CLUSTERQ__\n{self.squeue}\nRC {self.squeue_rc}\n"
            f"__SRUNX_{n}_CLUSTERA__\n{self.sacct}\nRC {self.sacct_rc}\n"
        )
        return out, "", self.code


def test_cluster_name_from_slurmctld_is_returned_and_padding_is_stripped() -> None:
    ex = FakeExecute("clusterA                \n")
    assert query_job_cluster(5, ex) == "clusterA"
    assert len(ex.commands) == 1  # one round trip


def test_cluster_slurmctld_answer_wins_over_accounting() -> None:
    assert query_job_cluster(5, FakeExecute("fromctld\n", "fromacct\n")) == "fromctld"


def test_cluster_falls_back_to_accounting_when_slurmctld_has_nothing() -> None:
    assert query_job_cluster(5, FakeExecute("", "clusterB\n")) == "clusterB"


def test_cluster_falls_back_to_accounting_when_squeue_fails() -> None:
    ex = FakeExecute("clusterA\n", "clusterB\n", squeue_rc=1)
    assert query_job_cluster(5, ex) == "clusterB"


def test_cluster_falls_back_when_slurmctld_names_are_mixed_or_invalid() -> None:
    assert query_job_cluster(5, FakeExecute("a\nb\n", "c\n")) == "c"
    assert query_job_cluster(5, FakeExecute("bad name;x\n", "c\n")) == "c"


def test_cluster_login_noise_is_ignored() -> None:
    assert query_job_cluster(5, FakeExecute("c1\n", noise="Welcome!\n")) == "c1"


def test_cluster_both_empty_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("", "")) is None


def test_cluster_mixed_names_everywhere_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("a\nb\n", "c\nd\n")) is None


def test_cluster_repeated_same_name_is_accepted() -> None:
    assert query_job_cluster(5, FakeExecute("a\na\n")) == "a"


def test_cluster_invalid_names_everywhere_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("bad name;rm\n", "also bad!\n")) is None


def test_cluster_timeout_rc_124_is_none() -> None:
    ex = FakeExecute("a\n", "b\n", squeue_rc=124, sacct_rc=124)
    assert query_job_cluster(5, ex) is None


def test_cluster_nonzero_overall_exit_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("a\n", "b\n", code=1)) is None


def test_cluster_missing_marker_is_none() -> None:
    assert query_job_cluster(5, lambda cmd: ("clusterA\nRC 0\n", "", 0)) is None


def test_cluster_missing_rc_line_is_none() -> None:
    def ex(cmd: str) -> tuple[str, str, int]:
        return f"__SRUNX_{_nonce(cmd)}_CLUSTERQ__\nclusterA\n", "", 0

    assert query_job_cluster(5, ex) is None


def test_cluster_execute_raising_is_none() -> None:
    def ex(cmd: str) -> tuple[str, str, int]:
        raise RuntimeError("ssh down")

    assert query_job_cluster(5, ex) is None


def test_cluster_without_runner_is_none() -> None:
    assert query_job_cluster(5, None) is None


def test_cluster_command_asks_slurmctld_then_accounting_once_each_bounded() -> None:
    cmd = build_cluster_command(42, "ab" * 8)
    assert cmd.index("squeue") < cmd.index("sacct")
    assert "squeue -h -j 42 -O Cluster" in cmd
    assert "sacct -n -P -X -j 42 -o Cluster" in cmd
    assert "command -v timeout" in cmd
    assert "timeout 10 squeue" in cmd and "timeout 10 sacct" in cmd
    assert cmd.count("squeue") == 2 and cmd.count("sacct") == 2  # one per branch
    assert "sleep" not in cmd
    assert "while" not in cmd and "until" not in cmd


def test_cluster_command_rejects_bad_nonce() -> None:
    with pytest.raises(ValueError):
        build_cluster_command(1, "x; true")


# ----------------------------------------------------------- job state queries

US = SQUEUE_SEP
NONCE = "ab" * 8


def sq(
    array_id: int, job_id: int, state: str, workdir: str = "/w", command: str = "run.sh"
) -> str:
    return US.join([str(array_id), str(job_id), state, workdir, command])


def section(name: str, body: str = "", rc: int = 0, nonce: str = NONCE) -> str:
    return f"__SRUNX_{nonce}_{name}__\n{body}\nRC {rc}\n"


def round1(
    squeue: str = "",
    *,
    sacct: dict[int, str] | None = None,
    sacct_rc: int = 0,
    config: str | None = None,
    config_rc: int = 0,
    date: str = "1800000000",
    squeue_rc: int = 0,
) -> str:
    out = section("DATE", date) + section("SQUEUE", squeue, squeue_rc)
    # By default one (empty) accounting section, as a query with ids has.
    for k, body in (sacct if sacct is not None else {0: ""}).items():
        out += section(f"SACCT{k}", body, sacct_rc)
    out += section("CONFIG", config or "", config_rc if config is not None else 1)
    return out


SACCT_DONE = "40070|COMPLETED|1794904226|clusterA"


class FakeCluster:
    """Answers round trip 1 / 2 from canned bodies, keyed on the nonce in the command."""

    def __init__(
        self,
        squeue: str = "",
        *,
        sacct: str | None = None,
        sacct_rc: int = 0,
        config: str | None = None,
        scontrol: dict[int, tuple[str, int]] | None = None,
        squeue_rc: int = 0,
        code: int = 0,
        stderr: str = "warning: some noise on stderr",
    ) -> None:
        self.stderr = stderr
        self.squeue, self.sacct, self.sacct_rc = squeue, sacct, sacct_rc
        self.config, self.scontrol = config, scontrol or {}
        self.squeue_rc, self.code = squeue_rc, code
        self.commands: list[str] = []

    def __call__(self, cmd: str) -> tuple[str, str, int]:
        self.commands.append(cmd)
        nonce = re.search(r"__SRUNX_([0-9a-f]+)_", cmd).group(1)  # type: ignore[union-attr]
        names = re.findall(rf"__SRUNX_{nonce}_(\w+?)__", cmd)
        out = "login banner\n"
        for name in names:
            if name == "DATE":
                out += section(name, "1800000000", nonce=nonce)
            elif name == "SQUEUE":
                out += section(name, self.squeue, self.squeue_rc, nonce)
            elif name.startswith("SACCT"):
                body = self.sacct if self.sacct is not None else ""
                out += section(name, body, self.sacct_rc, nonce)
            elif name == "CONFIG":
                out += section(
                    name,
                    self.config or "",
                    0 if self.config is not None else 126,
                    nonce,
                )
            elif name.startswith("SCONTROL"):
                body, rc = self.scontrol.get(int(name[8:]), ("", 1))
                out += section(name, body, rc, nonce)
        # stderr is noise the parser must never look at
        return out, self.stderr, self.code


class TestCommands:
    def test_queue_command_has_every_flag_and_the_real_separator_not_an_escape(
        self,
    ) -> None:
        cmd = build_queue_command([7], NONCE)
        assert "squeue -h -a -t all" in cmd
        assert "-t all" in cmd and " -a " in cmd
        for field in ("%F", "%A", "%T", "%Z", "%o"):
            assert field in cmd
        assert "SLURM_TIME_FORMAT=%s sacct" in cmd
        assert "-j 7" in cmd and "JobID,State,End,Cluster" in cmd
        assert "sinfo" not in cmd
        # The separator is produced by the remote shell; no literal \x1f text.
        assert "\\x1f" not in cmd
        assert "printf '\\037'" in cmd

    def test_queue_command_without_ids_has_no_sacct(self) -> None:
        cmd = build_queue_command([], NONCE)
        assert "sacct" not in cmd and "squeue" in cmd

    def test_queue_command_chunks_ids(self) -> None:
        ids = list(range(1, SACCT_CHUNK + 2))
        cmd = build_queue_command(ids, NONCE)
        assert cmd.count("sacct -n") == 2
        assert "SACCT0" in cmd and "SACCT1" in cmd

    def test_scontrol_command_is_per_id(self) -> None:
        cmd = build_scontrol_command([3, 1], NONCE)
        assert cmd.index("show job 1") < cmd.index("show job 3")

    def test_terminal_states_are_the_spec_set(self) -> None:
        assert TERMINAL_STATES == {
            "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL",
            "PREEMPTED", "OUT_OF_MEMORY", "BOOT_FAIL", "DEADLINE",
        }  # fmt: skip


class TestSqueue:
    def test_completed_squeue_row_is_still_active(self) -> None:
        rep, unresolved = parse_queue_output(round1(sq(5, 5, "COMPLETED")), [5], NONCE)
        assert rep.states[5].kind == "active"
        assert rep.states[5].state == "COMPLETED"
        assert rep.states[5].source == "squeue"
        assert unresolved == []

    def test_match_through_array_base_id_and_job_id(self) -> None:
        rows = sq(10, 11, "RUNNING") + "\n" + sq(10, 12, "PENDING")
        rep, _ = parse_queue_output(round1(rows), [10, 12, 99], NONCE)
        assert rep.states[10].kind == "active"
        assert rep.states[12].kind == "active"
        assert rep.states[99].kind == "unknown"

    def test_nonterminal_row_state_preferred(self) -> None:
        rows = sq(10, 11, "COMPLETED") + "\n" + sq(10, 12, "RUNNING")
        rep, _ = parse_queue_output(round1(rows), [10], NONCE)
        assert rep.states[10].state == "RUNNING"

    def test_workdir_and_command_with_pipes_and_spaces_parse(self) -> None:
        row = sq(5, 5, "RUNNING", "/a b/c|d", "sh -c 'x | y' z")
        rep, _ = parse_queue_output(round1(row), [5], NONCE)
        assert rep.error is None
        assert rep.queue[0].workdir == "/a b/c|d"
        assert rep.queue[0].command == "sh -c 'x | y' z"

    def test_queue_rows_returned_even_without_job_ids(self) -> None:
        rep = query_job_states([], FakeCluster(sq(5, 5, "RUNNING", "/w/x", "c.sh")))
        assert rep.error is None
        assert [(r.workdir, r.command) for r in rep.queue] == [("/w/x", "c.sh")]
        assert rep.states == {}

    @pytest.mark.parametrize(
        "row",
        [
            US.join(["5", "5", "RUNNING", "/w"]),
            US.join(["5", "5", "RUNNING", "/w", "c", "extra"]),
            US.join(["x", "5", "RUNNING", "/w", "c"]),
            US.join(["5", "5x", "RUNNING", "/w", "c"]),
            "plain garbage",
        ],
    )
    def test_unparsable_row_fails_everything_closed(self, row: str) -> None:
        good = sq(7, 7, "RUNNING")
        rep, unresolved = parse_queue_output(round1(good + "\n" + row), [7, 8], NONCE)
        assert rep.error is not None
        assert rep.queue == ()
        assert {s.kind for s in rep.states.values()} == {"unknown"}
        assert unresolved == []

    def test_squeue_nonzero_is_error(self) -> None:
        rep, _ = parse_queue_output(round1("", squeue_rc=1), [5], NONCE)
        assert rep.error and "squeue" in rep.error
        assert rep.states[5].kind == "unknown"

    def test_missing_squeue_section_is_error(self) -> None:
        rep, _ = parse_queue_output(section("DATE", "1"), [5], NONCE)
        assert rep.error

    @pytest.mark.parametrize(
        "stderr", ["", "warning: some noise on stderr", "error: x"]
    )
    def test_stderr_is_never_judged_only_stdout_and_exit_codes(
        self, stderr: str
    ) -> None:
        cluster = FakeCluster(sq(5, 5, "RUNNING"), stderr=stderr)
        rep = query_job_states([5], cluster)
        assert rep.error is None
        assert rep.states[5].kind == "active"

    def test_stderr_text_cannot_rescue_a_failed_squeue(self) -> None:
        cluster = FakeCluster("", squeue_rc=1, stderr="")
        assert query_job_states([5], cluster).error

    def test_execute_raising_is_error(self) -> None:
        def boom(cmd: str) -> tuple[str, str, int]:
            raise OSError("closed")

        rep = query_job_states([5], boom)
        assert rep.error and rep.states[5].kind == "unknown"

    def test_execute_exit_minus_one_is_error(self) -> None:
        rep = query_job_states([5], lambda cmd: ("", "", -1))
        assert rep.error

    def test_execute_none_is_error(self) -> None:
        rep = query_job_states([5], None)
        assert rep.error and rep.states[5].kind == "unknown"

    def test_now_is_parsed(self) -> None:
        rep, _ = parse_queue_output(round1("", date="1800000123"), [], NONCE)
        assert rep.now == 1800000123 and rep.error is None

    def test_missing_date_section_is_error_and_every_job_unknown(self) -> None:
        out = section("SQUEUE", sq(5, 5, "RUNNING")) + section("SACCT0", "")
        rep, _ = parse_queue_output(out, [5], NONCE)
        assert rep.error and "clock" in rep.error
        assert rep.states[5].kind == "unknown"

    @pytest.mark.parametrize("date", ["oops", ""])
    def test_unusable_clock_is_error(self, date: str) -> None:
        rep, _ = parse_queue_output(round1("", date=date), [5], NONCE)
        assert rep.error and rep.states[5].kind == "unknown"

    def test_missing_sacct_section_with_ids_is_error(self) -> None:
        rep, _ = parse_queue_output(round1("", sacct={}), [5], NONCE)
        assert rep.error and "sacct" in rep.error
        assert rep.states[5].kind == "unknown" and rep.queue == ()

    def test_missing_second_sacct_chunk_is_error(self) -> None:
        ids = list(range(1, SACCT_CHUNK + 2))
        rep, _ = parse_queue_output(round1("", sacct={0: ""}), ids, NONCE)
        assert rep.error

    def test_no_sacct_section_needed_without_ids(self) -> None:
        rep, _ = parse_queue_output(round1("", sacct={}), [], NONCE)
        assert rep.error is None

    def test_failed_sacct_section_is_still_not_an_error(self) -> None:
        rep, _ = parse_queue_output(round1("", sacct={0: ""}, sacct_rc=1), [5], NONCE)
        assert rep.error is None


class TestSacct:
    def _rep(self, rows: str, ids: list[int] | None = None, **kw: object):
        out = round1("", sacct={0: rows}, **kw)  # type: ignore[arg-type]
        return parse_queue_output(out, ids or [40070], NONCE)

    def test_terminal_row_with_all_fields(self) -> None:
        rep, unresolved = self._rep(SACCT_DONE)
        st = rep.states[40070]
        assert st.kind == "terminal" and st.state == "COMPLETED"
        assert st.ended_at == 1794904226
        assert st.cluster == "clusterA"
        assert st.source == "sacct"
        assert unresolved == []

    def test_non_integer_end_gives_none(self) -> None:
        rep, _ = self._rep("40070|COMPLETED|Unknown|clusterA")
        assert rep.states[40070].kind == "terminal"
        assert rep.states[40070].ended_at is None

    def test_cancelled_by_and_plus_suffix(self) -> None:
        rows = "1|CANCELLED by 1000|2|c\n2|COMPLETED+|2|c"
        rep, _ = self._rep(rows, [1, 2])
        assert rep.states[1].kind == "terminal" and rep.states[1].state == "CANCELLED"
        assert rep.states[2].kind == "terminal"

    def test_non_terminal_state_is_active(self) -> None:
        rep, _ = self._rep("40070|REQUEUED|Unknown|c")
        assert rep.states[40070].kind == "active"
        assert rep.states[40070].state == "REQUEUED"

    def test_array_with_one_nonterminal_element_is_active(self) -> None:
        rows = "9_1|COMPLETED|2|c\n9_2|RUNNING|Unknown|c"
        rep, _ = self._rep(rows, [9])
        assert rep.states[9].kind == "active"

    def test_array_rows_use_the_latest_end(self) -> None:
        rows = "9_1|COMPLETED|20|c\n9_2|FAILED|30|c"
        rep, _ = self._rep(rows, [9])
        st = rep.states[9]
        assert (st.kind, st.state, st.ended_at) == ("terminal", "FAILED", 30)

    def test_two_cluster_names_give_none(self) -> None:
        rep, _ = self._rep("9_1|COMPLETED|2|a\n9_2|COMPLETED|2|b", [9])
        assert rep.states[9].kind == "terminal" and rep.states[9].cluster is None

    def test_empty_cluster_gives_none(self) -> None:
        rep, _ = self._rep("9|COMPLETED|2|", [9])
        assert rep.states[9].cluster is None

    def test_squeue_row_wins_over_sacct(self) -> None:
        out = round1(sq(40070, 40070, "RUNNING"), sacct={0: SACCT_DONE})
        rep, _ = parse_queue_output(out, [40070], NONCE)
        assert rep.states[40070].kind == "active"
        assert rep.states[40070].source == "squeue"

    def test_one_malformed_row_makes_every_job_in_the_section_unknown(self) -> None:
        rows = SACCT_DONE + "\n" + "41|COMPLETED|garbage-without-enough-fields"
        rep, unresolved = self._rep(rows, [40070, 41])
        assert rep.error is None
        assert rep.states[40070].kind == "unknown"
        assert rep.states[41].kind == "unknown"
        assert unresolved == [41, 40070]

    def test_malformed_row_cannot_be_outvoted_by_a_terminal_row_of_the_same_job(
        self,
    ) -> None:
        rows = SACCT_DONE + "\n40070|RUNNING"  # the live row is the broken one
        rep, _ = self._rep(rows)
        assert rep.states[40070].kind == "unknown"

    def test_row_without_a_numeric_job_id_is_malformed(self) -> None:
        rows = SACCT_DONE + "\nx|COMPLETED|2|clusterA"
        rep, _ = self._rep(rows)
        assert rep.states[40070].kind == "unknown"

    def test_malformed_row_in_one_chunk_leaves_the_other_chunk_alone(self) -> None:
        ids = list(range(1, SACCT_CHUNK + 2))
        rows0 = "\n".join(f"{i}|COMPLETED|2|c" for i in ids[:SACCT_CHUNK])
        out = round1("", sacct={0: rows0, 1: "oops"})
        rep, unresolved = parse_queue_output(out, ids, NONCE)
        assert rep.states[1].kind == "terminal"
        assert rep.states[ids[-1]].kind == "unknown" and unresolved == [ids[-1]]

    def test_blank_lines_in_sacct_are_not_malformed(self) -> None:
        rep, _ = self._rep("\n" + SACCT_DONE + "\n\n")
        assert rep.states[40070].kind == "terminal"

    def test_row_with_wrong_field_count_is_ignored_and_unresolved(self) -> None:
        rep, unresolved = self._rep("40070|COMPLETED|1794904226")
        assert rep.states[40070].kind == "unknown"
        assert unresolved == [40070]

    def test_sacct_failure_leaves_ids_unresolved_without_error(self) -> None:
        rep, unresolved = self._rep(SACCT_DONE, sacct_rc=1)
        assert rep.error is None
        assert rep.states[40070].kind == "unknown"
        assert unresolved == [40070]

    def test_no_rows_for_an_id_is_unresolved(self) -> None:
        rep, unresolved = self._rep(SACCT_DONE, [40070, 41])
        assert rep.states[40070].kind == "terminal"
        assert unresolved == [41]

    def test_chunked_sacct_sections_are_all_read(self) -> None:
        ids = list(range(1, SACCT_CHUNK + 2))
        rows0 = "\n".join(f"{i}|COMPLETED|2|c" for i in ids[:SACCT_CHUNK])
        rows1 = f"{ids[-1]}|FAILED|2|c"
        out = round1("", sacct={0: rows0, 1: rows1})
        rep, unresolved = parse_queue_output(out, ids, NONCE)
        assert unresolved == []
        assert rep.states[1].kind == "terminal"
        assert rep.states[ids[-1]].state == "FAILED"


class TestScontrolAndMinJobAge:
    def test_scontrol_known_job_is_active_never_terminal_even_when_completed(
        self,
    ) -> None:
        cluster = FakeCluster(
            sacct_rc=1,
            scontrol={5: ("JobId=5 JobName=x JobState=COMPLETED Reason=None", 0)},
        )
        rep = query_job_states([5], cluster)
        st = rep.states[5]
        assert st.kind == "active" and st.state == "COMPLETED"
        assert st.source == "scontrol" and "slurmctld" in st.detail
        assert len(cluster.commands) == 2

    @pytest.mark.parametrize("rc", [127, 126, 1])
    def test_sacct_absent_or_failing_leaves_job_unknown_never_terminal(
        self, rc: int
    ) -> None:
        cluster = FakeCluster(sacct=SACCT_DONE, sacct_rc=rc, scontrol={})
        rep = query_job_states([40070], cluster)
        assert rep.error is None and rep.states[40070].kind == "unknown"

    def test_scontrol_available_and_denied_both_follow_the_rules(self) -> None:
        known = FakeCluster(sacct_rc=1, scontrol={5: ("JobState=PENDING", 0)})
        assert query_job_states([5], known).states[5].kind == "active"
        denied = FakeCluster(sacct_rc=1, scontrol={5: ("", 126)}, config=None)
        rep = query_job_states([5], denied)
        assert rep.states[5].kind == "unknown" and rep.min_job_age is None

    def test_min_job_age_readable_and_unreadable_via_query(self) -> None:
        ok = FakeCluster(config="MinJobAge = 600 sec\n")
        assert query_job_states([], ok).min_job_age == 600
        assert query_job_states([], FakeCluster()).min_job_age is None

    def test_scontrol_not_knowing_the_job_is_unknown(self) -> None:
        cluster = FakeCluster(sacct_rc=1, scontrol={5: ("slurm_load_jobs error", 1)})
        assert query_job_states([5], cluster).states[5].kind == "unknown"

    def test_scontrol_permission_denied_only_makes_the_job_unknown(self) -> None:
        cluster = FakeCluster(
            sacct_rc=1, scontrol={5: ("", 126)}, squeue=sq(7, 7, "RUNNING")
        )
        rep = query_job_states([5, 7], cluster)
        assert rep.error is None
        assert rep.states[5].kind == "unknown"
        assert rep.states[7].kind == "active"

    def test_second_round_trip_failure_keeps_jobs_unknown_without_error(self) -> None:
        calls = {"n": 0}
        first = FakeCluster(sacct_rc=1)

        def execute(cmd: str) -> tuple[str, str, int]:
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("dropped")
            return first(cmd)

        rep = query_job_states([5], execute)
        assert rep.error is None and rep.states[5].kind == "unknown"

    def test_no_second_round_trip_when_everything_resolved(self) -> None:
        cluster = FakeCluster(sq(5, 5, "RUNNING"))
        query_job_states([5], cluster)
        assert len(cluster.commands) == 1

    def test_parse_scontrol_output_directly(self) -> None:
        out = section("SCONTROL5", "JobId=5 JobState=RUNNING", 0, NONCE) + section(
            "SCONTROL6", "", 0, NONCE
        )
        got = parse_scontrol_output(out, [5, 6], NONCE)
        assert got[5].kind == "active" and got[6].kind == "unknown"

    def test_min_job_age_present(self) -> None:
        cfg = "AccountingStorageType = x\nMinJobAge              = 300 sec\n"
        rep, _ = parse_queue_output(round1("", config=cfg), [], NONCE)
        assert rep.min_job_age == 300

    def test_min_job_age_absent(self) -> None:
        rep, _ = parse_queue_output(round1("", config="Other = 1\n"), [], NONCE)
        assert rep.min_job_age is None

    def test_min_job_age_section_failed(self) -> None:
        cfg = "MinJobAge = 300 sec\n"
        rep, _ = parse_queue_output(round1("", config=cfg, config_rc=126), [], NONCE)
        assert rep.min_job_age is None and rep.error is None

    def test_login_noise_before_first_marker_is_ignored(self) -> None:
        out = "Last login: x\nMOTD |||\n" + round1(sq(5, 5, "RUNNING"))
        rep, _ = parse_queue_output(out, [5], NONCE)
        assert rep.error is None and rep.states[5].kind == "active"
