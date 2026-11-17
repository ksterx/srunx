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
    m = re.search(r"__SRUNX_([0-9a-f]+)_CLUSTER__", cmd)
    assert m
    return m.group(1)


class FakeExecute:
    """Answers the cluster command with *rows* and exit code *rc*."""

    def __init__(
        self, rows: str = "", rc: int = 0, *, code: int = 0, noise: str = ""
    ) -> None:
        self.rows, self.rc, self.code, self.noise = rows, rc, code, noise
        self.commands: list[str] = []

    def __call__(self, cmd: str) -> tuple[str, str, int]:
        self.commands.append(cmd)
        n = _nonce(cmd)
        out = f"{self.noise}__SRUNX_{n}_CLUSTER__\n{self.rows}\nRC {self.rc}\n"
        return out, "", self.code


def test_cluster_name_is_returned() -> None:
    ex = FakeExecute("gpucloud2\n")
    assert query_job_cluster(5, ex) == "gpucloud2"
    assert len(ex.commands) == 1


def test_cluster_login_noise_is_ignored() -> None:
    assert query_job_cluster(5, FakeExecute("c1\n", noise="Welcome!\n")) == "c1"


def test_cluster_empty_output_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("")) is None


def test_cluster_mixed_names_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("a\nb\n")) is None


def test_cluster_repeated_same_name_is_accepted() -> None:
    assert query_job_cluster(5, FakeExecute("a\na\n")) == "a"


def test_cluster_invalid_name_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("bad name;rm\n")) is None


def test_cluster_timeout_rc_124_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("gpucloud2\n", rc=124)) is None


def test_cluster_nonzero_exit_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("gpucloud2\n", code=1)) is None


def test_cluster_missing_marker_is_none() -> None:
    assert query_job_cluster(5, lambda cmd: ("gpucloud2\nRC 0\n", "", 0)) is None


def test_cluster_missing_rc_line_is_none() -> None:
    def ex(cmd: str) -> tuple[str, str, int]:
        return f"__SRUNX_{_nonce(cmd)}_CLUSTER__\ngpucloud2\n", "", 0

    assert query_job_cluster(5, ex) is None


def test_cluster_execute_raising_is_none() -> None:
    def ex(cmd: str) -> tuple[str, str, int]:
        raise RuntimeError("ssh down")

    assert query_job_cluster(5, ex) is None


def test_cluster_without_runner_is_none() -> None:
    assert query_job_cluster(5, None) is None


def test_cluster_command_is_one_bounded_attempt() -> None:
    cmd = build_cluster_command(42, "ab" * 8)
    assert "command -v timeout" in cmd
    assert "timeout 10 sacct" in cmd
    assert cmd.count("sacct") == 2  # one per branch of the if
    assert "-j 42" in cmd and "-o Cluster" in cmd
    assert "sleep" not in cmd
    assert "while" not in cmd and "until" not in cmd


def test_cluster_command_rejects_bad_nonce() -> None:
    with pytest.raises(ValueError):
        build_cluster_command(1, "x; rm -rf /")


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
    for k, body in (sacct or {}).items():
        out += section(f"SACCT{k}", body, sacct_rc)
    out += section("CONFIG", config or "", config_rc if config is not None else 1)
    return out


SACCT_DONE = "40070|COMPLETED|1794900000|1794904226|gpucloud2"


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
    ) -> None:
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
        return out, "slurm_load_partitions: Access/permission denied", self.code


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
        assert "-j 7" in cmd and "JobID,State,Submit,End,Cluster" in cmd
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

    def test_permission_denied_on_stderr_with_exit_zero_is_a_success(self) -> None:
        cluster = FakeCluster(sq(5, 5, "RUNNING"))
        rep = query_job_states([5], cluster)
        assert rep.error is None
        assert rep.states[5].kind == "active"

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

    def test_now_is_parsed_and_missing_gives_none(self) -> None:
        rep, _ = parse_queue_output(round1("", date="1800000123"), [], NONCE)
        assert rep.now == 1800000123
        rep, _ = parse_queue_output(round1("", date="oops"), [], NONCE)
        assert rep.now is None and rep.error is None


class TestSacct:
    def _rep(self, rows: str, ids: list[int] | None = None, **kw: object):
        out = round1("", sacct={0: rows}, **kw)  # type: ignore[arg-type]
        return parse_queue_output(out, ids or [40070], NONCE)

    def test_terminal_row_with_all_fields(self) -> None:
        rep, unresolved = self._rep(SACCT_DONE)
        st = rep.states[40070]
        assert st.kind == "terminal" and st.state == "COMPLETED"
        assert st.submitted_at == 1794900000
        assert st.ended_at == 1794904226
        assert st.cluster == "gpucloud2"
        assert st.source == "sacct"
        assert unresolved == []

    def test_non_integer_end_gives_none(self) -> None:
        rep, _ = self._rep("40070|COMPLETED|1794900000|Unknown|gpucloud2")
        assert rep.states[40070].kind == "terminal"
        assert rep.states[40070].ended_at is None

    @pytest.mark.parametrize("submit", ["", "Unknown", "2026-01-01T00:00:00"])
    def test_missing_or_non_integer_submit_gives_none(self, submit: str) -> None:
        rep, _ = self._rep(f"40070|COMPLETED|{submit}|1794904226|gpucloud2")
        assert rep.states[40070].submitted_at is None

    def test_cancelled_by_and_plus_suffix(self) -> None:
        rows = "1|CANCELLED by 1000|1|2|c\n2|COMPLETED+|1|2|c"
        rep, _ = self._rep(rows, [1, 2])
        assert rep.states[1].kind == "terminal" and rep.states[1].state == "CANCELLED"
        assert rep.states[2].kind == "terminal"

    def test_non_terminal_state_is_active(self) -> None:
        rep, _ = self._rep("40070|REQUEUED|1|Unknown|c")
        assert rep.states[40070].kind == "active"
        assert rep.states[40070].state == "REQUEUED"

    def test_array_with_one_nonterminal_element_is_active(self) -> None:
        rows = "9_1|COMPLETED|1|2|c\n9_2|RUNNING|1|Unknown|c"
        rep, _ = self._rep(rows, [9])
        assert rep.states[9].kind == "active"

    def test_array_rows_use_latest_end_and_earliest_submit(self) -> None:
        rows = "9_1|COMPLETED|10|20|c\n9_2|FAILED|5|30|c"
        rep, _ = self._rep(rows, [9])
        st = rep.states[9]
        assert (st.kind, st.state, st.ended_at, st.submitted_at) == (
            "terminal", "FAILED", 30, 5,
        )  # fmt: skip

    def test_two_cluster_names_give_none(self) -> None:
        rep, _ = self._rep("9_1|COMPLETED|1|2|a\n9_2|COMPLETED|1|2|b", [9])
        assert rep.states[9].kind == "terminal" and rep.states[9].cluster is None

    def test_empty_cluster_gives_none(self) -> None:
        rep, _ = self._rep("9|COMPLETED|1|2|", [9])
        assert rep.states[9].cluster is None

    def test_squeue_row_wins_over_sacct(self) -> None:
        out = round1(sq(40070, 40070, "RUNNING"), sacct={0: SACCT_DONE})
        rep, _ = parse_queue_output(out, [40070], NONCE)
        assert rep.states[40070].kind == "active"
        assert rep.states[40070].source == "squeue"

    def test_row_with_wrong_field_count_is_ignored_and_unresolved(self) -> None:
        rep, unresolved = self._rep("40070|COMPLETED|1794904226|gpucloud2")
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
        rows0 = "\n".join(f"{i}|COMPLETED|1|2|c" for i in ids[:SACCT_CHUNK])
        rows1 = f"{ids[-1]}|FAILED|1|2|c"
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
