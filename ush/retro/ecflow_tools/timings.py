#!/usr/bin/env python3
"""Summarize retro cycle wall time, Slurm queue wait, and job runtime.

The ecFlow attempt logs identify each submitted Slurm job by ECF_RID. Slurm accounting
provides the submit, start, end, elapsed, and state fields needed to separate time spent
waiting for a resource from time spent executing on the compute nodes.
"""

import argparse
from collections import defaultdict
from datetime import datetime
import fnmatch
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys


ATTEMPT_SUFFIX = re.compile(r"^(?P<task>.+)\.(?P<try>\d+)$")
ECF_RID_PATTERN = re.compile(r"(?:export\s+)?ECF_RID=(\d+)")
ECF_NAME_PATTERN = re.compile(r"(?:export\s+)?ECF_NAME=(/[^\s\"']+)")
PDY_PATTERN = re.compile(r"(?:export\s+)?PDY=(\d{8})(?:\s|$)")
CYCLE_PATTERN = re.compile(r"/(\d{2}z)(?:/|$)")
ECFLOW_STATE_PATTERN = re.compile(
    r"\b(active|complete|suspended|queued|submitted|aborted|unknown)\b", re.I
)
SBATCH_TIME_PATTERN = re.compile(r"^\s*#SBATCH\s+--time(?:=|\s+)(\S+)")
PBS_TIME_PATTERN = re.compile(r"^\s*#PBS\s+-l\s+walltime=(\S+)")
SBATCH_NODES_PATTERN = re.compile(r"^\s*#SBATCH\s+--nodes(?:=|\s+)(\d+)")
PBS_SELECT_PATTERN = re.compile(r"^\s*#PBS\s+-l\s+select=(\d+)")


def parse_timestamp(value):
    """Parse Slurm's local timestamp, treating its unknown markers as missing."""
    if not value or value in {"Unknown", "None", "N/A", "(null)"}:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_elapsed(value):
    """Parse Slurm elapsed time, including its days-HH:MM:SS form."""
    if not value or value in {"Unknown", "None", "N/A", "(null)"}:
        return None
    match = re.fullmatch(r"(?:(\d+)-)?(\d+):(\d{2}):(\d{2})(?:\.\d+)?", value)
    if not match:
        return None
    days, hours, minutes, seconds = match.groups()
    return (int(days or 0) * 86400 + int(hours) * 3600 +
            int(minutes) * 60 + int(seconds))


def parse_job_request(attempt_log, task, try_number):
    """Read the effective scheduler request from this attempt's generated job card."""
    job_card = attempt_log.with_name(f"{task}.job{try_number}")
    if not job_card.is_file():
        return None, None

    sbatch_time = pbs_time = sbatch_nodes = pbs_nodes = None
    try:
        for line in job_card.read_text(errors="replace").splitlines():
            if sbatch_time is None:
                match = SBATCH_TIME_PATTERN.match(line)
                if match:
                    sbatch_time = parse_elapsed(match.group(1))
            if pbs_time is None:
                match = PBS_TIME_PATTERN.match(line)
                if match:
                    pbs_time = parse_elapsed(match.group(1))
            if sbatch_nodes is None:
                match = SBATCH_NODES_PATTERN.match(line)
                if match:
                    sbatch_nodes = int(match.group(1))
            if pbs_nodes is None:
                match = PBS_SELECT_PATTERN.match(line)
                if match:
                    pbs_nodes = int(match.group(1))
    except OSError:
        return None, None

    # Ursa uses Slurm. Fall back to the PBS request when reading WCOSS2 job cards.
    return (sbatch_time if sbatch_time is not None else pbs_time,
            sbatch_nodes if sbatch_nodes is not None else pbs_nodes)


def format_seconds(value):
    """Format seconds as d-HH:MM:SS when long, otherwise HH:MM:SS."""
    if value is None:
        return "--"
    value = max(0, int(round(value)))
    days, value = divmod(value, 86400)
    hours, value = divmod(value, 3600)
    minutes, seconds = divmod(value, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


def parse_attempt_log(path, suite_root):
    """Read the small startup portion of one ecFlow attempt log."""
    suffix_match = ATTEMPT_SUFFIX.match(path.name)
    if not suffix_match:
        return None

    job_id = ecf_name = pdy = None
    try:
        # ECF_RID, ECF_NAME, and PDY are exported near job startup; stop reading once found.
        with path.open("r", errors="replace") as stream:
            for line_number, line in enumerate(stream):
                if job_id is None:
                    match = ECF_RID_PATTERN.search(line)
                    if match:
                        job_id = match.group(1)
                if ecf_name is None:
                    match = ECF_NAME_PATTERN.search(line)
                    if match:
                        ecf_name = match.group(1)
                if pdy is None:
                    match = PDY_PATTERN.search(line)
                    if match:
                        pdy = match.group(1)
                if job_id and ecf_name and (pdy or line_number >= 10000):
                    break
    except OSError:
        return None

    if not job_id:
        return None

    node_path = ecf_name or str(path.relative_to(suite_root))
    cycle_match = CYCLE_PATTERN.search(node_path)
    cycle = cycle_match.group(1) if cycle_match else "unknown"
    if not pdy:
        pdy_match = re.search(r"(?:^|/)PDY[=_](\d{8})(?:/|$)", node_path)
        pdy = pdy_match.group(1) if pdy_match else "unknown"

    task = suffix_match.group("task")
    try_number = int(suffix_match.group("try"))
    requested_walltime, requested_nodes = parse_job_request(path, task, try_number)
    return {
        "job_id": job_id,
        "task": task,
        "try": try_number,
        "pdy": pdy,
        "cycle": cycle,
        "node_path": node_path,
        "log": str(path),
        "requested_walltime": requested_walltime,
        "requested_nodes": requested_nodes,
    }


def find_attempts(suite_root):
    """Find ecFlow attempt logs and associate them with their Slurm job IDs."""
    attempts = []
    for path in suite_root.rglob("*"):
        if not path.is_file() or not ATTEMPT_SUFFIX.match(path.name):
            continue
        attempt = parse_attempt_log(path, suite_root)
        if attempt:
            attempts.append(attempt)
    return attempts


def query_sacct(job_ids):
    """Query Slurm accounting in bounded batches and return top-level job records."""
    if not shutil.which("sacct"):
        raise RuntimeError("sacct was not found; load the Slurm environment and retry")

    records = {}
    failures = []
    batch_size = 100
    for start in range(0, len(job_ids), batch_size):
        batch = job_ids[start:start + batch_size]
        command = [
            "sacct", "-n", "-P", "-j", ",".join(batch),
            "--format=JobIDRaw,JobName,Submit,Start,End,Elapsed,State",
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=90)
        except (OSError, subprocess.SubprocessError) as error:
            failures.append(str(error))
            continue
        if result.returncode != 0:
            message = result.stderr.strip() or f"sacct exited {result.returncode}"
            failures.append(message)
            continue

        requested = set(batch)
        for line in result.stdout.splitlines():
            fields = line.split("|")
            if len(fields) != 7 or fields[0] not in requested:
                # Slurm also returns .batch, .extern, and application-step rows; only the
                # top-level job row represents queue wait and whole-job elapsed time.
                continue
            records[fields[0]] = {
                "job_name": fields[1],
                "submit": parse_timestamp(fields[2]),
                "start": parse_timestamp(fields[3]),
                "end": parse_timestamp(fields[4]),
                "elapsed": parse_elapsed(fields[5]),
                "state": fields[6],
            }

    if not records and failures:
        raise RuntimeError("Could not read Slurm accounting: " + failures[0])
    return records, failures


def add_timing_fields(attempts, accounting, now):
    """Calculate each attempt's observed queue and execution durations."""
    timed = []
    for attempt in attempts:
        record = accounting.get(attempt["job_id"])
        if not record:
            continue
        submit = record["submit"]
        start = record["start"]
        end = record["end"]
        state = record["state"]

        if submit and start:
            queue_seconds = (start - submit).total_seconds()
        elif submit and end:
            # Jobs cancelled before starting have waited until their end time, but did no work.
            queue_seconds = (end - submit).total_seconds()
        elif submit:
            # A still-pending job's queue wait is measured up to report time.
            queue_seconds = (now - submit).total_seconds()
        else:
            queue_seconds = None

        if record["elapsed"] is not None:
            run_seconds = record["elapsed"]
        elif start and end:
            run_seconds = (end - start).total_seconds()
        elif start and not end and state.startswith("RUNNING"):
            run_seconds = (now - start).total_seconds()
        else:
            run_seconds = 0

        timed.append({
            **attempt,
            **record,
            "queue_seconds": max(0, queue_seconds) if queue_seconds is not None else None,
            "run_seconds": max(0, run_seconds) if run_seconds is not None else None,
        })
    return timed


def wall_span(rows, now, through_now=False):
    """Return first-submit to final-end, or to now while the suite is unfinished."""
    submits = [row["submit"] for row in rows if row["submit"]]
    if not submits:
        return None
    if through_now:
        final_end = now
    else:
        finishes = [row["end"] for row in rows if row["end"]]
        if not finishes:
            return None
        final_end = max(finishes)
    return (final_end - min(submits)).total_seconds()


def config_value(config_path, name, default):
    """Read a simple NAME=${NAME:-value} setting without executing the config shell."""
    try:
        config_text = config_path.read_text(errors="replace")
    except OSError:
        return default
    match = re.search(rf"^{re.escape(name)}=\$\{{{re.escape(name)}:-([^}}]+)\}}", config_text, re.M)
    return match.group(1).strip() if match else default


def query_ecflow_suite_state(suite, config_path):
    """Read the suite state so pending ecFlow work extends an in-progress wall span."""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", suite):
        return None, "suite name contains unsupported characters"

    machine = os.environ.get("MACHINE") or config_value(config_path, "MACHINE", "URSA")
    host = os.environ.get("ECF_HOST") or config_value(config_path, "ECFLOW_HOST", "uecflow01")
    port = os.environ.get("ECF_PORT")
    if not port:
        base_port = 2000 if machine == "WCOSS2" else 1500
        port = str(base_port + os.getuid())
    version = config_value(config_path, "ECFLOW_VER", "5.11.4")

    env = os.environ.copy()
    env["ECF_HOST"] = host
    env["ECF_PORT"] = str(port)
    client = shutil.which("ecflow_client")
    if client:
        command = [client, f"--get_state=/{suite}"]
    else:
        # timings.sh may run before the ecFlow module is loaded in the user's shell.
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", version):
            return None, "invalid ECFLOW_VER in the retro config"
        env["RETRO_TIMINGS_ECF_HOST"] = host
        env["RETRO_TIMINGS_ECF_PORT"] = str(port)
        command = [
            "bash", "-lc",
            f'module load "ecflow/{version}" >/dev/null 2>&1 && '
            'export ECF_HOST="$RETRO_TIMINGS_ECF_HOST" '
            'ECF_PORT="$RETRO_TIMINGS_ECF_PORT" && '
            f'ecflow_client --get_state=/{suite}',
        ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=20, env=env)
    except (OSError, subprocess.SubprocessError) as error:
        return None, str(error)
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "ecFlow state query failed"
        return None, message

    match = ECFLOW_STATE_PATTERN.search(result.stdout)
    if not match:
        return None, "ecFlow returned no recognizable suite state"
    return match.group(1).lower(), None


def print_group_table(title, groups, key_name, out):
    print(f"\n{title}", file=out)
    print(f"{'GROUP':<42} {'JOBS':>6} {'QUEUE SUM':>12} {'QUEUE AVG':>12} "
          f"{'RUN SUM':>12} {'RUN AVG':>12}", file=out)
    print("-" * 100, file=out)
    for key, rows in sorted(groups.items()):
        queue_values = [row["queue_seconds"] for row in rows
                        if row["queue_seconds"] is not None]
        run_values = [row["run_seconds"] for row in rows
                      if row["run_seconds"] is not None]
        queue_sum = sum(queue_values) if queue_values else None
        queue_avg = statistics.mean(queue_values) if queue_values else None
        run_sum = sum(run_values) if run_values else None
        run_avg = statistics.mean(run_values) if run_values else None
        label = key if len(key) <= 41 else "..." + key[-38:]
        print(f"{label:<42} {len(rows):>6} {format_seconds(queue_sum):>12} "
              f"{format_seconds(queue_avg):>12} {format_seconds(run_sum):>12} "
              f"{format_seconds(run_avg):>12}", file=out)


def retro_config_paths():
    """Find the selected retro config and its domain-specific job-size table."""
    repo = Path(__file__).resolve().parents[3]
    config_path = Path(os.environ.get(
        "RETRO_CONFIG", str(repo / "ecf/defs/retro/config.sh")
    )).expanduser().resolve()
    try:
        config_text = config_path.read_text(errors="replace")
    except OSError:
        config_text = ""

    domain = os.environ.get("DOMAIN")
    if not domain:
        match = re.search(r"^DOMAIN=\$\{DOMAIN:-([^}]+)\}", config_text, re.M)
        domain = match.group(1).strip() if match else "RRFS_NA_3km"
    domain_path = repo / "ecf/defs/retro/domains" / f"{domain}.sh"
    return config_path, domain, domain_path


def domain_job_patterns(domain_path):
    """Read existing DOMAIN_SBATCH task patterns and options without sourcing shell code."""
    try:
        lines = domain_path.read_text(errors="replace").splitlines()
    except OSError:
        return []

    in_table = False
    patterns = []
    for line_number, line in enumerate(lines, 1):
        if not in_table and re.search(r"\bDOMAIN_SBATCH\s*=\s*\(", line):
            in_table = True
            continue
        if in_table and line.strip() == ")":
            break
        if not in_table:
            continue
        match = re.search(r'"([^"\n]+)"', line)
        if not match or ":" not in match.group(1):
            continue
        pattern, options = match.group(1).split(":", 1)
        patterns.append({
            "pattern": pattern,
            "options": options,
            "line": line_number,
        })
    return patterns


def suggested_task_pattern(task):
    """Collapse forecast leads and member numbers into a useful DOMAIN_SBATCH glob."""
    pattern = re.sub(r"(_f)\d{3}.*$", r"\1*", task)
    pattern = re.sub(r"(_f)\d+(?=_|$).*", r"\1*", pattern)
    pattern = re.sub(r"(_mem)\d+", r"\1*", pattern)
    if pattern == task:
        pattern = re.sub(r"_\d+$", "_*", task)
    return pattern


def walltime_target(task, domain_patterns):
    """Map a job type to the config variable or domain table that controls its limit."""
    if fnmatch.fnmatchcase(task, "jrrfs_det_analysis_jedi*"):
        return {"kind": "config", "key": "WALLTIME_ANALYSIS_JEDI"}
    if fnmatch.fnmatchcase(task, "jrrfs_enkf_analysis_jedi*"):
        return {"kind": "config", "key": "WALLTIME_ANALYSIS_ENKF_JEDI"}

    matches = [item for item in domain_patterns
               if fnmatch.fnmatchcase(task, item["pattern"])]
    if matches:
        # Use the narrowest existing glob when more than one table entry matches.
        target = max(matches, key=lambda item: len(
            item["pattern"].replace("*", "").replace("?", "")
        ))
        return {"kind": "domain", **target}
    return {"kind": "domain", "pattern": suggested_task_pattern(task), "options": ""}


def walltime_recommendations(rows, config_path, domain_path, domain_patterns):
    """Estimate conservative limits from completed and timed-out runtime samples."""
    groups = defaultdict(list)
    for row in rows:
        target = walltime_target(row["task"], domain_patterns)
        key = (target["kind"], target.get("key", target.get("pattern", "")))
        groups[key].append((target, row))

    recommendations = []
    for (kind, target_name), entries in sorted(groups.items()):
        samples = []
        completed = 0
        timeouts = 0
        card_walltimes = []
        current_nodes = []
        for target, row in entries:
            if row["requested_walltime"] is not None:
                card_walltimes.append(row["requested_walltime"])
            if row["requested_nodes"] is not None:
                current_nodes.append(row["requested_nodes"])
            state = row["state"].split()[0].split("+")[0]
            if state == "COMPLETED":
                completed += 1
                if row["run_seconds"] and row["run_seconds"] > 0:
                    samples.append(row["run_seconds"])
            elif state == "TIMEOUT":
                timeouts += 1
                if row["run_seconds"] and row["run_seconds"] > 0:
                    samples.append(row["run_seconds"])

        if not samples:
            continue

        # Keep 25 percent headroom above the slowest observed successful or timed-out run,
        # then round up to a five-minute increment so the suggestion is easy to edit.
        recommendation = max(300, math.ceil(max(samples) * 1.25 / 300) * 300)
        target = entries[0][0]
        # DOMAIN_SBATCH and the JEDI config become submit-time overrides; job cards retain
        # their original #SBATCH values, so prefer the active override for CURRENT.
        current_override = None
        if kind == "config":
            current_value = os.environ.get(target_name) or config_value(
                config_path, target_name, ""
            )
            current_override = parse_elapsed(current_value.strip().strip("\"'"))
        else:
            time_match = re.search(r"--time(?:=|\s+)(\S+)", target.get("options", ""))
            if time_match:
                current_override = parse_elapsed(time_match.group(1))
        current_limits = ([current_override] if current_override is not None
                          else card_walltimes)

        if kind == "config":
            location = str(config_path)
            snippet = f"{target_name}=${{{target_name}:-{format_seconds(recommendation)}}}"
        else:
            options = target.get("options", "")
            options = re.sub(r"--time(?:=|\s+)\S+", "", options).strip()
            if not re.search(r"--nodes(?:=|\s+)", options) and current_nodes:
                nodes = statistics.mode(current_nodes)
                options = f"{options} --nodes={nodes}".strip()
            options = f"{options} --time={format_seconds(recommendation)}".strip()
            location = f"{domain_path}: DOMAIN_SBATCH"
            snippet = f'"{target_name}:{options}"'

        unique_limits = sorted(set(current_limits))
        # Keep the report focused on actionable changes when the current limit is known.
        if len(unique_limits) == 1 and unique_limits[0] == recommendation:
            continue
        if len(unique_limits) == 1:
            current = format_seconds(unique_limits[0])
        elif unique_limits:
            current = "mixed"
        else:
            current = "unknown"

        recommendations.append({
            "target": target_name,
            "kind": kind,
            "location": location,
            "snippet": snippet,
            "samples": len(samples),
            "completed": completed,
            "timeouts": timeouts,
            "current": current,
            "maximum": max(samples),
            "recommended": recommendation,
        })
    return recommendations


def print_walltime_recommendations(recommendations):
    """Print copy-ready settings and sample-quality notes without editing config files."""
    if not recommendations:
        return
    print("\nWalltime recommendations")
    print("Basis: completed and timed-out Slurm runtimes; maximum observed plus 25 percent, "
          "rounded up to five minutes. Suggestions do not edit config files.")
    print("CURRENT uses the retro config/domain override when present; otherwise it uses the task-card limit.")
    print(f"{'TASK PATTERN / VARIABLE':<38} {'N':>5} {'DONE':>6} {'TIMEOUT':>8} "
          f"{'CURRENT':>10} {'MAX RUN':>10} {'SUGGESTED':>10}")
    print("-" * 97)
    for item in recommendations:
        label = item["target"] if len(item["target"]) <= 37 else "..." + item["target"][-34:]
        print(f"{label:<38} {item['samples']:>5} {item['completed']:>6} "
              f"{item['timeouts']:>8} {item['current']:>10} "
              f"{format_seconds(item['maximum']):>10} "
              f"{format_seconds(item['recommended']):>10}")
        print(f"  Set in {item['location']}")
        print(f"  {item['snippet']}")
        if item["samples"] < 5:
            print("  Low sample count; treat this estimate as preliminary.")
        if item["timeouts"]:
            print("  Includes timed-out runs; review these before adopting the suggestion.")


def main():
    parser = argparse.ArgumentParser(
        description="Report retro wall time, Slurm queue wait, and per-job execution time."
    )
    parser.add_argument("--suite", required=True,
                        help="RRFS ecFlow suite name (defaults from the retro config)")
    parser.add_argument("--ecf-home", required=True,
                        help="ecFlow submit directory (defaults from the retro config)")
    parser.add_argument("--detail", action="store_true",
                        help="list each Slurm job attempt after the summaries")
    args = parser.parse_args()

    suite_root = Path(args.ecf_home).expanduser() / args.suite
    if not suite_root.is_dir():
        sys.exit(f"No ecFlow job directory found for /{args.suite}: {suite_root}")

    attempts = find_attempts(suite_root)
    if not attempts:
        sys.exit(f"No ecFlow attempt logs with Slurm job IDs found under {suite_root}")

    job_ids = sorted({attempt["job_id"] for attempt in attempts}, key=int)
    try:
        accounting, failures = query_sacct(job_ids)
    except RuntimeError as error:
        sys.exit(f"timings: {error}")

    now = datetime.now()
    rows = add_timing_fields(attempts, accounting, now)
    if not rows:
        sys.exit("No attempt logs matched Slurm accounting records. Check accounting retention and job IDs.")

    matched = {row["job_id"] for row in rows}
    unmatched = [attempt for attempt in attempts if attempt["job_id"] not in matched]
    queue_values = [row["queue_seconds"] for row in rows
                    if row["queue_seconds"] is not None]
    run_values = [row["run_seconds"] for row in rows
                  if row["run_seconds"] is not None]
    config_path, domain, domain_path = retro_config_paths()
    suite_state, state_error = query_ecflow_suite_state(args.suite, config_path)
    unfinished_jobs = any(not row["end"] for row in rows)
    suite_in_progress = suite_state in {"active", "suspended", "queued", "submitted"}
    through_now = suite_in_progress or (suite_state is None and unfinished_jobs)
    span = wall_span(rows, now, through_now=through_now)

    print(f"Retro timing report: /{args.suite}")
    print(f"Submit directory: {suite_root}")
    print(f"EcFlow attempts found: {len(attempts)}; Slurm records matched: {len(rows)}; "
          f"unmatched: {len(unmatched)}")
    if suite_state:
        print(f"EcFlow suite state: {suite_state}")
    if span is not None:
        if suite_in_progress:
            label = f"through report time; ecFlow suite {suite_state}"
        elif suite_state is None and unfinished_jobs:
            label = "through report time; Slurm job still unfinished"
        elif suite_state:
            label = f"from first submit to last end; ecFlow suite {suite_state}"
        else:
            label = "from first submit to last end; ecFlow state unavailable"
        print(f"Full retro wall span ({label}): {format_seconds(span)}")
    if queue_values:
        print(f"Queue wait summed across jobs: {format_seconds(sum(queue_values))}; "
              f"mean {format_seconds(statistics.mean(queue_values))}; "
              f"median {format_seconds(statistics.median(queue_values))}; "
              f"max {format_seconds(max(queue_values))}")
    if run_values:
        print(f"Job runtime summed across jobs: {format_seconds(sum(run_values))}; "
              f"mean {format_seconds(statistics.mean(run_values))}; "
              f"median {format_seconds(statistics.median(run_values))}; "
              f"max {format_seconds(max(run_values))}")
    if through_now:
        print(f"Timing extends through report time {now:%Y-%m-%d %H:%M:%S}.")
    if suite_state is None:
        print(f"Warning: could not query ecFlow suite state ({state_error}); "
              "when no Slurm job is unfinished, the span may omit gaps between jobs.")
    if unmatched:
        print("Warning: some logged job IDs were not returned by sacct; they are excluded "
              "from timing totals.")
    if failures:
        print(f"Warning: {len(failures)} sacct batch query(s) failed; some jobs may be missing.")

    by_cycle = defaultdict(list)
    by_task = defaultdict(list)
    for row in rows:
        by_cycle[(row["pdy"], row["cycle"])].append(row)
        by_task[row["task"]].append(row)

    cycle_groups = {f"{pdy} {cycle}": group
                    for (pdy, cycle), group in by_cycle.items()}
    print_group_table("By cycle", cycle_groups, "cycle", sys.stdout)
    print_group_table("By task name", by_task, "task", sys.stdout)

    patterns = domain_job_patterns(domain_path)
    recommendations = walltime_recommendations(
        rows, config_path, domain_path, patterns
    )
    print(f"\nRetro config: {config_path} (DOMAIN={domain})")
    print_walltime_recommendations(recommendations)

    if args.detail:
        print("\nJob attempts", file=sys.stdout)
        print(f"{'CYCLE':<17} {'TASK':<45} {'JOB ID':>12} {'TRY':>4} "
              f"{'STATE':<18} {'QUEUE':>12} {'RUNTIME':>12}")
        print("-" * 126)
        for row in sorted(rows, key=lambda item: (
                item["submit"] or datetime.max, item["pdy"], item["cycle"], item["task"])):
            cycle = f"{row['pdy']} {row['cycle']}"
            task = row["task"] if len(row["task"]) <= 44 else "..." + row["task"][-41:]
            print(f"{cycle:<17} {task:<45} {row['job_id']:>12} {row['try']:>4} "
                  f"{row['state']:<18} {format_seconds(row['queue_seconds']):>12} "
                  f"{format_seconds(row['run_seconds']):>12}")

    if unmatched and args.detail:
        print("\nUnmatched job IDs", file=sys.stdout)
        for attempt in unmatched:
            print(f"{attempt['job_id']}  {attempt['pdy']} {attempt['cycle']} "
                  f"{attempt['task']} try {attempt['try']}")


if __name__ == "__main__":
    main()
