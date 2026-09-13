#!/usr/bin/env python3
"""
OCF CRM - Monday.com GIRLS SECOND PASS
======================================

Purpose
-------
Migrate the historical Girls participant records that were deliberately left
out by the first clean OCF Monday migration.

Expected use after the first pass:
- 68 clean participants already exist in OCF CRM.
- This script skips exact existing Name + DOB matches.
- Remaining historical/incomplete participants are inserted as FullPending.
- Different participant names sharing DOB/contact details are treated as a
  HOUSEHOLD WARNING, not an automatic duplicate.
- Missing optional/incomplete guardian/emergency/consent details do not stop
  the participant itself being migrated.
- Missing participant mobile/postcode are stored as empty strings because the
  current OCF schema declares those columns NOT NULL. The record is FullPending
  and the issue is clearly recorded in the audit CSV and audit log.
- Safeguarding concern CONTENT is NOT copied into audit_logs. Only a boolean
  warning that the source contains safeguarding information is recorded.
- No existing participant is updated or overwritten.

IMPORTANT
---------
This file expects the existing first-pass script to be in the SAME folder:

    migrate_ocf_monday_participants.py

That lets this second pass reuse the exact Excel mapping already tested in the
first migration.

Run PREVIEW first:

    py -3.14 .\migrate_ocf_monday_participants_second_pass.py `
      --source ".\Girls Participant List.xlsx" `
      --batch girls

Only after preview is checked:

    py -3.14 .\migrate_ocf_monday_participants_second_pass.py `
      --source ".\Girls Participant List.xlsx" `
      --batch girls `
      --commit

Connection
----------
Uses the same environment variable as V1:

    OCF_MYSQL_CONNECTION_STRING

Example:
    $rawPassword = "YOUR_PASSWORD"
    $encodedPassword = [uri]::EscapeDataString($rawPassword)
    $env:OCF_MYSQL_CONNECTION_STRING="mysql://USER:$encodedPassword@IP:3306/DB?charset=utf8mb4"
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pymysql

# Reuse the already-tested first-pass mapping and insert routines.
try:
    import migrate_ocf_monday_participants as base
except ModuleNotFoundError as exc:
    raise SystemExit(
        "ERROR: migrate_ocf_monday_participants.py must be in the same folder "
        "as this second-pass script."
    ) from exc


MIGRATION_NAME = "OCF_MONDAY_PARTICIPANTS_V2_PENDING"
AUDIT_ACTION = "ParticipantMigratedMonday"

# These were blockers in V1 because V1 only inserted complete/clean records.
# In V2 they become FullPending issues instead of blocking participant creation.
SOFT_BLOCKER_PREFIXES = (
    "Could not extract a valid UK postcode",
    "Date Form Completed is missing/invalid",
    "Under-18 participant has no valid Parent/Guardian mobile",
    "Under-18 participant has no Parent/Guardian Name",
    "No participant mobile exists in Monday export",
    "Under-18 participant does not have a complete emergency contact",
    "Safeguarding Concerns contains meaningful data",
    "Conflicting Photo/Video Consent values",
)


@dataclass
class SecondPassRow:
    participant: Any
    source_row: int
    pending_issues: list[str] = field(default_factory=list)
    hard_blockers: list[str] = field(default_factory=list)
    existing_exact: list[dict[str, Any]] = field(default_factory=list)
    household_matches: list[dict[str, Any]] = field(default_factory=list)
    safeguarding_present: bool = False
    target_status: str = "FullPending"
    result: str = "PENDING"
    preview_ocf_id: str | None = None
    inserted_ocf_id: str | None = None

    @property
    def can_insert(self) -> bool:
        return not self.existing_exact and not self.hard_blockers


# ---------------------------------------------------------------------------
# Phone normalisation
# ---------------------------------------------------------------------------

def normalize_uk_mobile_v2(value: Any, *, redacted_sample: bool = False) -> str | None:
    """
    Handles normal UK mobile formats AND Excel numeric cells that lost the
    leading zero, e.g. 7712345678 -> +447712345678.
    """
    if value is None:
        return None

    if isinstance(value, float):
        if value.is_integer():
            text = str(int(value))
        else:
            text = str(value)
    elif isinstance(value, int):
        text = str(value)
    else:
        text = base.clean_text(value, redacted_sample=redacted_sample)

    if not text:
        return None

    compact = re.sub(r"[\s().\-]", "", text)

    if re.fullmatch(r"07\d{9}", compact):
        return "+44" + compact[1:]

    if re.fullmatch(r"7\d{9}", compact):
        return "+44" + compact

    if re.fullmatch(r"\+447\d{9}", compact):
        return compact

    if re.fullmatch(r"447\d{9}", compact):
        return "+" + compact

    if re.fullmatch(r"00447\d{9}", compact):
        return "+" + compact[2:]

    if re.fullmatch(r"\+4407\d{9}", compact):
        return "+44" + compact[4:]

    return None


# Make base.prepare_row use the corrected phone normaliser.
base.normalize_uk_mobile = normalize_uk_mobile_v2


# ---------------------------------------------------------------------------
# Existing participant / household checks
# ---------------------------------------------------------------------------

def _basic_participant(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "ocfId": row["ocf_id"],
        "fullName": row["full_name"],
        "dateOfBirth": str(row["date_of_birth"]),
        "mobileNumber": row["mobile_number"],
    }


def find_exact_existing(conn, full_name: str, dob: str) -> list[dict[str, Any]]:
    """
    Strong duplicate/already-migrated rule:
    Exact normalised Name + DOB only.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id,ocf_id,full_name,date_of_birth,mobile_number
            FROM participants
            WHERE LOWER(TRIM(full_name)) = LOWER(TRIM(%s))
              AND date_of_birth = %s
            ORDER BY participant_number
            """,
            (full_name, dob),
        )
        return [_basic_participant(r) for r in cur.fetchall()]


def find_household_matches(
    conn,
    full_name: str,
    dob: str,
    contacts: list[str],
) -> list[dict[str, Any]]:
    """
    Same DOB/contact but DIFFERENT name is NOT treated as a duplicate.
    It is surfaced as a household/twin/sibling warning.
    """
    contacts = sorted({c for c in contacts if c})
    if not contacts:
        return []

    matches: dict[str, dict[str, Any]] = {}

    def add(rows, matched_by: str):
        for row in rows:
            # Exact same name is handled by find_exact_existing().
            if str(row["full_name"]).strip().casefold() == full_name.strip().casefold():
                continue

            key = str(row["id"])
            item = matches.setdefault(
                key,
                {
                    **_basic_participant(row),
                    "matchedBy": [],
                },
            )
            if matched_by not in item["matchedBy"]:
                item["matchedBy"].append(matched_by)

    placeholders = ",".join(["%s"] * len(contacts))

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id,ocf_id,full_name,date_of_birth,mobile_number
            FROM participants
            WHERE date_of_birth=%s
              AND mobile_number IN ({placeholders})
            """,
            [dob, *contacts],
        )
        add(cur.fetchall(), "participant_mobile+dob")

        cur.execute(
            f"""
            SELECT DISTINCT
                p.id,p.ocf_id,p.full_name,p.date_of_birth,p.mobile_number
            FROM participants p
            INNER JOIN participant_guardians g
                ON g.participant_id=p.id
            WHERE p.date_of_birth=%s
              AND g.phone_number IN ({placeholders})
            """,
            [dob, *contacts],
        )
        add(cur.fetchall(), "guardian_phone+dob")

        cur.execute(
            f"""
            SELECT DISTINCT
                p.id,p.ocf_id,p.full_name,p.date_of_birth,p.mobile_number
            FROM participants p
            INNER JOIN emergency_contacts e
                ON e.participant_id=p.id
            WHERE p.date_of_birth=%s
              AND e.phone_number IN ({placeholders})
            """,
            [dob, *contacts],
        )
        add(cur.fetchall(), "emergency_phone+dob")

    return list(matches.values())


# ---------------------------------------------------------------------------
# Prepare V2 record
# ---------------------------------------------------------------------------

def is_soft_blocker(message: str) -> bool:
    return any(message.startswith(prefix) for prefix in SOFT_BLOCKER_PREFIXES)


def prepare_second_pass(
    sheet,
    source_row: int,
    excel_row,
    batch: str,
) -> SecondPassRow:
    p = base.prepare_row(
        sheet,
        source_row,
        excel_row,
        batch,
        allow_guardian_phone_for_adults=False,
    )

    v2 = SecondPassRow(participant=p, source_row=source_row)

    # Move V1's intentionally conservative blockers into pending issues.
    for blocker in list(p.blockers):
        if is_soft_blocker(blocker):
            v2.pending_issues.append(blocker)
        else:
            v2.hard_blockers.append(blocker)

    # V2's base insert checks p.ready, so leave only true hard blockers there.
    p.blockers = list(v2.hard_blockers)
    p.duplicate_matches = []

    # Reconstruct source safeguarding flag without copying the sensitive text.
    source_safeguarding = base.meaningful_safeguarding_concern(
        sheet.value(excel_row, "Safeguarding Concerns"),
        redacted_sample=sheet.redacted_sample,
    )
    v2.safeguarding_present = bool(source_safeguarding)
    if v2.safeguarding_present:
        safe_message = (
            "Source contains safeguarding information; content was NOT copied "
            "to audit metadata. Manual safeguarding follow-up is required."
        )
        if safe_message not in v2.pending_issues:
            v2.pending_issues.append(safe_message)

    # A conflicting Photo/Video source means "unknown/not resolved".
    # Never guess Granted or Declined.
    if any("Conflicting Photo/Video Consent" in x for x in v2.pending_issues):
        p.photo_video_consent = None

    # Current participants table requires postcode NOT NULL.
    # Empty string means genuinely missing legacy data; it does NOT invent a postcode.
    if not p.postcode:
        p.postcode = ""
        issue = "Historical postcode is missing/invalid; participant requires completion."
        if issue not in v2.pending_issues:
            v2.pending_issues.append(issue)

    # Current participants table requires mobile_number NOT NULL.
    # Prefer a real historical contact if one exists.
    if not p.mobile_number:
        if p.guardian_phone:
            p.mobile_number = p.guardian_phone
            p.mobile_source = "guardian_pending"
            v2.pending_issues.append(
                "Participant primary contact uses guardian mobile; participant mobile was not present in Monday export."
            )
        elif p.emergency_phone:
            p.mobile_number = p.emergency_phone
            p.mobile_source = "emergency_contact_pending"
            v2.pending_issues.append(
                "Participant primary contact uses emergency-contact mobile; participant/guardian mobile was not available."
            )
        else:
            # NOT NULL schema: use empty string, not a fabricated telephone number.
            p.mobile_number = ""
            p.mobile_source = "missing"
            v2.pending_issues.append(
                "No usable historical contact mobile is available; participant requires completion."
            )

    # Missing date_form_completed is permitted by the database.
    # We leave it NULL and mark FullPending.
    if not p.date_form_completed:
        issue = (
            "Historical Date Form Completed is unknown. date_form_completed remains NULL; "
            "created_at will reflect migration time until manually corrected."
        )
        if issue not in v2.pending_issues:
            v2.pending_issues.append(issue)

    # Under-18 guardian/emergency data can be incomplete in the historical source.
    # Child rows are inserted only when enough fields exist; missing pieces remain pending.
    if p.age_at_form is not None and p.age_at_form < 18:
        if not (p.guardian_name and p.guardian_phone):
            issue = "Guardian record is incomplete and will not be inserted until completed."
            if issue not in v2.pending_issues:
                v2.pending_issues.append(issue)

        if not (p.emergency_name and p.emergency_relationship and p.emergency_phone):
            issue = "Emergency contact is incomplete and will not be inserted until completed."
            if issue not in v2.pending_issues:
                v2.pending_issues.append(issue)

    # These second-pass records are intentionally FullPending whenever there is
    # any unresolved data-quality issue.
    v2.target_status = "FullPending" if v2.pending_issues else "Full"

    # Make all pending issues visible in the existing V1 audit metadata safely.
    for issue in v2.pending_issues:
        warning = f"[FULLPENDING] {issue}"
        if warning not in p.warnings:
            p.warnings.append(warning)

    return v2


# ---------------------------------------------------------------------------
# CSV / reporting
# ---------------------------------------------------------------------------

def household_text(matches: list[dict[str, Any]]) -> str:
    parts = []
    for m in matches:
        reason = ",".join(m.get("matchedBy", []))
        parts.append(f"{m['ocfId']}:{m['fullName']} ({reason})")
    return "; ".join(parts)


def existing_text(matches: list[dict[str, Any]]) -> str:
    return "; ".join(f"{m['ocfId']}:{m['fullName']}" for m in matches)


def csv_row(v2: SecondPassRow) -> dict[str, Any]:
    p = v2.participant
    return {
        "source_row": v2.source_row,
        "batch": p.batch,
        "result": v2.result,
        "target_registration_status": v2.target_status,
        "full_name": p.full_name,
        "date_of_birth": p.date_of_birth or "",
        "date_form_completed": p.date_form_completed or "",
        "gender": p.gender or "",
        "postcode": p.postcode or "",
        "mobile_source": p.mobile_source or "",
        "preview_ocf_id": v2.preview_ocf_id or "",
        "inserted_ocf_id": v2.inserted_ocf_id or "",
        "already_existing": existing_text(v2.existing_exact),
        "household_warning": household_text(v2.household_matches),
        "pending_issues": " | ".join(v2.pending_issues),
        "hard_blockers": " | ".join(v2.hard_blockers),
        "safeguarding_present": "YES" if v2.safeguarding_present else "NO",
        "monday_status": p.source_status or "",
        "warnings": " | ".join(p.warnings),
    }


def write_csv(path: Path, rows: list[SecondPassRow]) -> None:
    fieldnames = list(csv_row(rows[0]).keys()) if rows else ["source_row", "result"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(csv_row(row))


def classify(v2: SecondPassRow, committed: bool = False) -> str:
    if v2.existing_exact:
        return "ALREADY_MIGRATED"
    if v2.hard_blockers:
        return "HARD_BLOCKED"
    if committed and v2.inserted_ocf_id:
        return "INSERTED_FULL_PENDING" if v2.target_status == "FullPending" else "INSERTED_FULL"
    if v2.target_status == "FullPending":
        return "READY_FULL_PENDING"
    return "READY_FULL"


def print_summary(source: Path, batch: str, rows: list[SecondPassRow], output: Path, commit: bool):
    from collections import Counter

    counts = Counter(r.result for r in rows)

    print()
    print("OCF Monday Participant Migration - SECOND PASS")
    print("------------------------------------------------")
    print(f"Source:                         {source}")
    print(f"Batch:                          {batch}")
    print(f"Source participant rows:        {len(rows)}")
    print(f"Already migrated / existing:    {counts['ALREADY_MIGRATED']}")
    print(f"Ready FullPending:              {counts['READY_FULL_PENDING']}")
    print(f"Ready Full:                     {counts['READY_FULL']}")
    print(f"Hard blocked:                   {counts['HARD_BLOCKED']}")
    print(f"Inserted FullPending:           {counts['INSERTED_FULL_PENDING']}")
    print(f"Inserted Full:                  {counts['INSERTED_FULL']}")
    print(f"Household warnings:             {sum(bool(r.household_matches) for r in rows)}")
    print(f"Safeguarding-source flags:      {sum(r.safeguarding_present for r in rows)}")
    print(f"Audit CSV:                      {output}")
    print()

    if commit:
        print("SECOND-PASS COMMIT COMPLETE.")
        print("Existing participants were not overwritten.")
    else:
        print("SECOND-PASS PREVIEW COMPLETE: database was NOT changed.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def args_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OCF Monday historical participant second-pass migration."
    )
    parser.add_argument("--source", required=True)
    parser.add_argument(
        "--batch",
        required=True,
        choices=["girls", "boys", "mixed"],
    )
    parser.add_argument(
        "--connection-string",
        default=os.getenv(
            "OCF_MYSQL_CONNECTION_STRING",
            "mysql://YOUR_DB_USER:YOUR_DB_PASSWORD@YOUR_DB_HOST:3306/YOUR_DB_NAME?charset=utf8mb4",
        ),
    )
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--redacted-sample", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = args_parser()

    source = Path(args.source).expanduser().resolve()
    if not source.exists():
        print(f"ERROR: source file not found: {source}", file=sys.stderr)
        return 2

    if "YOUR_DB_" in args.connection_string:
        print(
            "ERROR: set OCF_MYSQL_CONNECTION_STRING or pass --connection-string.",
            file=sys.stderr,
        )
        return 3

    # Tell the reused insert routine to record this as the V2 migration.
    base.MIGRATION_NAME = MIGRATION_NAME
    base.AUDIT_ACTION = AUDIT_ACTION

    try:
        conn = base.connect_mysql(args.connection_string)
    except Exception as exc:
        print(f"ERROR connecting to MySQL: {exc}", file=sys.stderr)
        return 3

    try:
        base.check_schema(conn)
        sheet = base.MondaySheet(source, redacted_sample=args.redacted_sample)

        prepared: list[SecondPassRow] = []
        source_rows: dict[int, Any] = {}

        for source_row, excel_row in sheet.participant_rows():
            source_rows[source_row] = excel_row
            v2 = prepare_second_pass(sheet, source_row, excel_row, args.batch)

            p = v2.participant

            if p.full_name and p.date_of_birth:
                v2.existing_exact = find_exact_existing(
                    conn,
                    p.full_name,
                    p.date_of_birth,
                )

                contacts = [
                    p.guardian_phone,
                    p.emergency_phone,
                    p.mobile_number if p.mobile_number else None,
                ]
                v2.household_matches = find_household_matches(
                    conn,
                    p.full_name,
                    p.date_of_birth,
                    contacts,
                )

                if v2.household_matches and not v2.existing_exact:
                    issue = (
                        "Another participant with the same DOB shares a household/contact number. "
                        "This is treated as a household warning, not an automatic duplicate."
                    )
                    if issue not in v2.pending_issues:
                        v2.pending_issues.append(issue)
                    if f"[FULLPENDING] {issue}" not in p.warnings:
                        p.warnings.append(f"[FULLPENDING] {issue}")
                    v2.target_status = "FullPending"

            v2.result = classify(v2, committed=False)
            prepared.append(v2)

        # Simulate OCF IDs for preview rows only.
        next_number = base.current_next_participant_number(conn)
        for v2 in prepared:
            p = v2.participant
            if v2.can_insert and p.first_name and p.last_name:
                try:
                    v2.preview_ocf_id = base.ocf_id(
                        p.first_name,
                        p.last_name,
                        next_number,
                    )
                    next_number += 1
                except Exception as exc:
                    v2.hard_blockers.append(f"OCF ID generation failed: {exc}")
                    p.blockers = list(v2.hard_blockers)
                    v2.result = classify(v2, committed=False)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        mode = "commit" if args.commit else "preview"
        output = source.parent / f"ocf_monday_{args.batch}_second_pass_{mode}_{timestamp}.csv"

        if args.commit:
            conn.begin()
            try:
                for v2 in prepared:
                    p = v2.participant

                    # Recheck exact duplicate INSIDE the transaction.
                    if p.full_name and p.date_of_birth:
                        v2.existing_exact = find_exact_existing(
                            conn,
                            p.full_name,
                            p.date_of_birth,
                        )

                    if v2.existing_exact or v2.hard_blockers:
                        v2.result = classify(v2, committed=False)
                        continue

                    # Household warnings never block. Refresh them for audit.
                    contacts = [
                        p.guardian_phone,
                        p.emergency_phone,
                        p.mobile_number if p.mobile_number else None,
                    ]
                    v2.household_matches = find_household_matches(
                        conn,
                        p.full_name,
                        p.date_of_birth,
                        contacts,
                    )

                    # The reused insert function reads this module-level value.
                    base.REGISTRATION_STATUS = v2.target_status

                    # Ensure V1 "ready" check only sees true hard blockers.
                    p.blockers = list(v2.hard_blockers)
                    p.duplicate_matches = []

                    _, ocf = base.insert_participant(conn, p)
                    v2.inserted_ocf_id = ocf
                    v2.result = classify(v2, committed=True)

                conn.commit()
            except Exception:
                conn.rollback()
                raise
        else:
            conn.rollback()

        # Reclassify preview-only rows after all checks.
        if not args.commit:
            for v2 in prepared:
                v2.result = classify(v2, committed=False)

        write_csv(output, prepared)
        print_summary(source, args.batch, prepared, output, args.commit)

        if any(r.hard_blockers for r in prepared):
            return 10

        return 0

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
