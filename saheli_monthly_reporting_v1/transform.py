\
from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


UNKNOWN_LOCATION = "Unknown / Unassigned"
UNKNOWN_CATEGORY = "Uncategorised"


def load_location_aliases(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if not {"Source", "CanonicalLocation"}.issubset(df.columns):
        raise ValueError(
            "location_aliases.csv must contain Source and CanonicalLocation columns."
        )
    return {
        str(row.Source).strip().casefold(): str(row.CanonicalLocation).strip()
        for row in df.itertuples(index=False)
        if str(row.Source).strip()
    }


def canonicalize(series: pd.Series, aliases: dict[str, str]) -> pd.Series:
    def one(value):
        if pd.isna(value) or not str(value).strip():
            return UNKNOWN_LOCATION
        raw = str(value).strip()
        return aliases.get(raw.casefold(), raw)
    return series.apply(one)


def add_period(df: pd.DataFrame, date_col: str, period) -> pd.DataFrame:
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    dates = out[date_col].dt.date
    out["Period"] = np.where(
        (dates >= period.previous_start) & (dates < period.previous_end_exclusive),
        "Previous",
        np.where(
            (dates >= period.report_start) & (dates < period.report_end_exclusive),
            "Current",
            None,
        ),
    )
    return out[out["Period"].notna()].copy()


def prepare_data(sessions, attendance, registrations, assessments, period, aliases):
    sessions = add_period(sessions, "SessionDate", period)
    attendance = add_period(attendance, "SessionDate", period)
    registrations = registrations.copy()
    assessments = assessments.copy()

    sessions["Location"] = canonicalize(sessions["VenueName"], aliases)
    attendance["Location"] = canonicalize(attendance["VenueName"], aliases)
    registrations["Location"] = canonicalize(registrations["Site"], aliases)
    assessments["Location"] = canonicalize(assessments["Site"], aliases)

    sessions["ActivityCategoryResolved"] = (
        sessions["ActivityCategory"]
        .fillna("")
        .astype(str)
        .str.strip()
    )
    fallback_category = sessions["Category"].fillna("").astype(str).str.strip()
    sessions.loc[
        sessions["ActivityCategoryResolved"].eq(""),
        "ActivityCategoryResolved",
    ] = fallback_category
    sessions.loc[
        sessions["ActivityCategoryResolved"].eq(""),
        "ActivityCategoryResolved",
    ] = UNKNOWN_CATEGORY

    sessions["CategoryResolved"] = sessions["Category"].fillna("").astype(str).str.strip()
    sessions.loc[sessions["CategoryResolved"].eq(""), "CategoryResolved"] = UNKNOWN_CATEGORY

    sessions["SubCategoryResolved"] = (
        sessions["SubCategory"].fillna("").astype(str).str.strip()
    )
    sessions.loc[
        sessions["SubCategoryResolved"].eq(""),
        "SubCategoryResolved",
    ] = "No subcategory"

    def member_key(row):
        if pd.notna(row.get("ParticipantId")):
            try:
                return f"FULL:{int(row['ParticipantId'])}"
            except Exception:
                return f"FULL:{row['ParticipantId']}"
        if pd.notna(row.get("LiteMemberId")):
            return f"LITE:{row['LiteMemberId']}"
        display = str(row.get("MemberDisplayId") or "").strip()
        if display:
            return f"OTHER:{display}"
        return None

    attendance["MemberKey"] = attendance.apply(member_key, axis=1)
    attendance["AttendedBool"] = attendance["Attended"].fillna(False).astype(bool)

    registrations["RegistrationDateResolved"] = pd.to_datetime(
        registrations["RegistrationDate"], errors="coerce"
    ).fillna(pd.to_datetime(registrations["CreatedAt"], errors="coerce"))
    registrations = add_period(registrations, "RegistrationDateResolved", period)

    assessments["AssessmentDate"] = pd.to_datetime(
        assessments["AssessmentDate"], errors="coerce"
    )

    return sessions, attendance, registrations, assessments


def _count_unique(series: pd.Series) -> int:
    return int(series.dropna().nunique())


def overall_summary(sessions, attendance, registrations, assessments, period):
    rows = []

    for label in ["Previous", "Current"]:
        s = sessions[(sessions["Period"] == label)]
        delivered = s[~s["IsCancelled"].fillna(False).astype(bool)]
        a = attendance[
            (attendance["Period"] == label)
            & attendance["AttendedBool"]
        ]
        r = registrations[registrations["Period"] == label]

        start = period.previous_start if label == "Previous" else period.report_start
        end = period.previous_end_exclusive if label == "Previous" else period.report_end_exclusive
        aa = assessments[
            (assessments["AssessmentDate"].dt.date >= start)
            & (assessments["AssessmentDate"].dt.date < end)
        ]

        rows.append({
            "Period": label,
            "Sessions Delivered": int(delivered["SessionId"].nunique()),
            "Cancelled Sessions": int(s[s["IsCancelled"].fillna(False).astype(bool)]["SessionId"].nunique()),
            "Attendance": int(len(a)),
            "Unique Participants": _count_unique(a["MemberKey"]),
            "New Registrations": int(r["ParticipantId"].nunique()),
            "Health Assessments": int(aa["AssessmentId"].nunique()),
            "Unique Assessed": _count_unique(aa["SaheliCardNumber"]),
            "Follow-up Assessments": int((aa["AssessmentNumber"].fillna(0) > 1).sum()),
        })

    wide = pd.DataFrame(rows).set_index("Period").T.reset_index(names="Metric")
    wide = wide.rename(columns={"Previous": "Previous Month", "Current": "Current Month"})
    wide["Change"] = wide["Current Month"] - wide["Previous Month"]
    wide["% Change"] = np.where(
        wide["Previous Month"].eq(0),
        np.nan,
        (wide["Change"] / wide["Previous Month"] * 100).round(1),
    )
    wide["Trend"] = np.select(
        [wide["Change"] > 0, wide["Change"] < 0],
        ["UP", "DOWN"],
        default="STABLE",
    )
    return wide


def location_summary(sessions, attendance):
    rows = []
    all_locations = sorted(set(sessions["Location"].dropna()) | set(attendance["Location"].dropna()))

    for location in all_locations:
        row = {"Location": location}
        for label, prefix in [("Previous", "Previous"), ("Current", "Current")]:
            s = sessions[
                (sessions["Location"] == location)
                & (sessions["Period"] == label)
            ]
            delivered = s[~s["IsCancelled"].fillna(False).astype(bool)]
            a = attendance[
                (attendance["Location"] == location)
                & (attendance["Period"] == label)
                & attendance["AttendedBool"]
            ]
            row[f"{prefix} Sessions"] = int(delivered["SessionId"].nunique())
            row[f"{prefix} Attendance"] = int(len(a))
            row[f"{prefix} Unique Participants"] = _count_unique(a["MemberKey"])

        row["Session Change"] = row["Current Sessions"] - row["Previous Sessions"]
        row["Attendance Change"] = row["Current Attendance"] - row["Previous Attendance"]
        row["Unique Change"] = (
            row["Current Unique Participants"] - row["Previous Unique Participants"]
        )
        row["Attendance % Change"] = (
            round(row["Attendance Change"] / row["Previous Attendance"] * 100, 1)
            if row["Previous Attendance"]
            else np.nan
        )
        row["Trend"] = (
            "UP" if row["Attendance Change"] > 0
            else "DOWN" if row["Attendance Change"] < 0
            else "STABLE"
        )
        rows.append(row)

    return pd.DataFrame(rows).sort_values(
        ["Current Attendance", "Location"], ascending=[False, True]
    )


def category_summary(sessions, attendance):
    delivered = sessions[~sessions["IsCancelled"].fillna(False).astype(bool)].copy()

    session_group = (
        delivered.groupby(
            ["Period", "Location", "ActivityCategoryResolved", "CategoryResolved", "SubCategoryResolved"],
            dropna=False,
        )["SessionId"]
        .nunique()
        .reset_index(name="Sessions")
    )

    # Attach category fields to attendance through SessionId to avoid trusting duplicated labels.
    session_dim = delivered[
        [
            "SessionId", "Period", "Location",
            "ActivityCategoryResolved", "CategoryResolved", "SubCategoryResolved",
        ]
    ].drop_duplicates("SessionId")

    attended = attendance[attendance["AttendedBool"]].merge(
        session_dim,
        on=["SessionId", "Period", "Location"],
        how="inner",
    )

    attendance_group = (
        attended.groupby(
            ["Period", "Location", "ActivityCategoryResolved", "CategoryResolved", "SubCategoryResolved"],
            dropna=False,
        )
        .agg(
            Attendance=("AttendanceId", "count"),
            UniqueParticipants=("MemberKey", lambda x: x.dropna().nunique()),
        )
        .reset_index()
    )

    merged = session_group.merge(
        attendance_group,
        on=["Period", "Location", "ActivityCategoryResolved", "CategoryResolved", "SubCategoryResolved"],
        how="left",
    )
    merged[["Attendance", "UniqueParticipants"]] = merged[
        ["Attendance", "UniqueParticipants"]
    ].fillna(0)

    keys = ["Location", "ActivityCategoryResolved", "CategoryResolved", "SubCategoryResolved"]
    parts = []
    for metric in ["Sessions", "Attendance", "UniqueParticipants"]:
        pivot = merged.pivot_table(
            index=keys, columns="Period", values=metric, aggfunc="sum", fill_value=0
        ).reset_index()
        for c in ["Previous", "Current"]:
            if c not in pivot.columns:
                pivot[c] = 0
        pivot = pivot.rename(
            columns={"Previous": f"Previous {metric}", "Current": f"Current {metric}"}
        )
        parts.append(pivot)

    out = parts[0]
    for p in parts[1:]:
        out = out.merge(p, on=keys, how="outer")

    out["Attendance Change"] = out["Current Attendance"] - out["Previous Attendance"]
    out["Trend"] = np.select(
        [out["Attendance Change"] > 0, out["Attendance Change"] < 0],
        ["UP", "DOWN"],
        default="STABLE",
    )
    return out.sort_values(keys)


def activity_summary(sessions, attendance):
    current_sessions = sessions[
        (sessions["Period"] == "Current")
        & (~sessions["IsCancelled"].fillna(False).astype(bool))
    ].copy()

    sgroup = (
        current_sessions.groupby(
            ["Location", "ActivityCategoryResolved", "ActivityName"], dropna=False
        )["SessionId"]
        .nunique()
        .reset_index(name="Sessions")
    )

    dim = current_sessions[
        ["SessionId", "Location", "ActivityCategoryResolved", "ActivityName"]
    ].drop_duplicates("SessionId")
    a = attendance[
        (attendance["Period"] == "Current")
        & attendance["AttendedBool"]
    ].merge(dim, on=["SessionId", "Location", "ActivityName"], how="inner")

    agroup = (
        a.groupby(
            ["Location", "ActivityCategoryResolved", "ActivityName"], dropna=False
        )
        .agg(
            Attendance=("AttendanceId", "count"),
            UniqueParticipants=("MemberKey", lambda x: x.dropna().nunique()),
        )
        .reset_index()
    )

    out = sgroup.merge(
        agroup,
        on=["Location", "ActivityCategoryResolved", "ActivityName"],
        how="left",
    ).fillna({"Attendance": 0, "UniqueParticipants": 0})
    out["Average Attendance / Session"] = (
        out["Attendance"] / out["Sessions"].replace(0, np.nan)
    ).round(2)
    return out.sort_values(
        ["Location", "ActivityCategoryResolved", "Attendance"],
        ascending=[True, True, False],
    )


def registration_summary(registrations):
    grouped = (
        registrations.groupby(["Period", "Location"])["ParticipantId"]
        .nunique()
        .reset_index(name="Registrations")
    )
    pivot = grouped.pivot_table(
        index="Location", columns="Period", values="Registrations", fill_value=0
    ).reset_index()
    for c in ["Previous", "Current"]:
        if c not in pivot.columns:
            pivot[c] = 0
    pivot = pivot.rename(
        columns={"Previous": "Previous Registrations", "Current": "Current Registrations"}
    )
    pivot["Change"] = pivot["Current Registrations"] - pivot["Previous Registrations"]
    return pivot.sort_values(
        ["Current Registrations", "Location"], ascending=[False, True]
    )


def demographic_tables(registrations):
    current = registrations[registrations["Period"] == "Current"].copy()

    gender = (
        current.assign(
            Gender=current["Gender"].fillna("").astype(str).str.strip().replace("", "Not recorded")
        )
        .groupby(["Location", "Gender"])
        .size()
        .reset_index(name="Participants")
    )

    ethnicity = (
        current.assign(
            Ethnicity=current["Ethnicity"].fillna("").astype(str).str.strip().replace("", "Not recorded")
        )
        .groupby(["Location", "Ethnicity"])
        .size()
        .reset_index(name="Participants")
    )

    def age_band(v):
        if pd.isna(v):
            return "Not recorded"
        try:
            age = int(v)
        except Exception:
            return "Not recorded"
        if age < 18:
            return "Under 18"
        if age <= 25:
            return "18-25"
        if age <= 35:
            return "26-35"
        if age <= 50:
            return "36-50"
        if age <= 60:
            return "51-60"
        if age <= 70:
            return "61-70"
        if age <= 80:
            return "71-80"
        return "80+"

    current["Age Band"] = current["Age"].apply(age_band)
    age = current.groupby(["Location", "Age Band"]).size().reset_index(name="Participants")

    disability = (
        current.assign(
            Disability=current["HasHealthConditionOrDisability"]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "Not recorded")
        )
        .groupby(["Location", "Disability"])
        .size()
        .reset_index(name="Participants")
    )

    return gender, ethnicity, age, disability


def assessment_activity_summary(assessments, period):
    current = assessments[
        (assessments["AssessmentDate"].dt.date >= period.report_start)
        & (assessments["AssessmentDate"].dt.date < period.report_end_exclusive)
    ].copy()
    previous = assessments[
        (assessments["AssessmentDate"].dt.date >= period.previous_start)
        & (assessments["AssessmentDate"].dt.date < period.previous_end_exclusive)
    ].copy()

    rows = []
    for location in sorted(set(current["Location"]) | set(previous["Location"])):
        row = {"Location": location}
        for df, prefix in [(previous, "Previous"), (current, "Current")]:
            x = df[df["Location"] == location]
            row[f"{prefix} Assessments"] = int(x["AssessmentId"].nunique())
            row[f"{prefix} Unique Assessed"] = int(x["SaheliCardNumber"].dropna().nunique())
            row[f"{prefix} Follow-ups"] = int((x["AssessmentNumber"].fillna(0) > 1).sum())
        row["Assessment Change"] = row["Current Assessments"] - row["Previous Assessments"]
        rows.append(row)
    return pd.DataFrame(rows)


def outcome_summary(assessments, period):
    """
    Mirrors the existing CRM ReportsService outcome direction:
      - ConfidenceToJoin: higher is improvement
      - FeelingConfident: higher is improvement
      - Movement: higher is improvement
      - FeelIsolated: lower is improvement
      - ActiveDaysPerWeek: higher is improvement
      - Fitness/physical activity: Movement OR ActiveDaysPerWeek improvement

    The result is attributed to the Site on the participant's latest
    assessment in the report month.
    """
    a = assessments.copy()
    a = a.sort_values(
        ["AssessmentDate", "AssessmentNumber", "AssessmentId"],
        ascending=[True, True, True],
    )

    def key(row):
        if pd.notna(row.get("ParticipantId")):
            try:
                return f"PID:{int(row['ParticipantId'])}"
            except Exception:
                return f"PID:{row['ParticipantId']}"
        card = str(row.get("SaheliCardNumber") or "").strip()
        return f"CARD:{card}" if card else f"AID:{row['AssessmentId']}"

    a["MemberKey"] = a.apply(key, axis=1)

    in_month = a[
        (a["AssessmentDate"].dt.date >= period.report_start)
        & (a["AssessmentDate"].dt.date < period.report_end_exclusive)
    ].copy()

    if in_month.empty:
        return pd.DataFrame(columns=[
            "Location", "Participants With Earlier Assessment",
            "Confidence Paired", "Improved Confidence To Join",
            "Feeling Confident Paired", "Improved Feeling Confident",
            "Movement Paired", "Improved Movement",
            "Isolation Paired", "Less Isolated",
            "Active Days Paired", "More Active Days",
            "Improved Fitness / Physical Activity",
        ])

    latest = (
        in_month.sort_values(
            ["AssessmentDate", "AssessmentNumber", "AssessmentId"],
            ascending=[False, False, False],
        )
        .drop_duplicates("MemberKey", keep="first")
    )

    outcome_rows = []

    for latest_row in latest.itertuples(index=False):
        member_key = latest_row.MemberKey
        history = a[
            (a["MemberKey"] == member_key)
            & (
                (a["AssessmentDate"] < latest_row.AssessmentDate)
                | (
                    (a["AssessmentDate"] == latest_row.AssessmentDate)
                    & (a["AssessmentNumber"] < latest_row.AssessmentNumber)
                )
                | (
                    (a["AssessmentDate"] == latest_row.AssessmentDate)
                    & (a["AssessmentNumber"] == latest_row.AssessmentNumber)
                    & (a["AssessmentId"] < latest_row.AssessmentId)
                )
            )
        ]

        if history.empty:
            continue

        baseline = history.iloc[0]  # earliest earlier assessment, matching backend logic

        def pair(metric):
            b = baseline.get(metric)
            l = getattr(latest_row, metric)
            return pd.notna(b) and pd.notna(l), b, l

        confidence_pair, b_conf, l_conf = pair("ConfidenceToJoin")
        feeling_pair, b_feel, l_feel = pair("FeelingConfident")
        movement_pair, b_move, l_move = pair("Movement")
        isolation_pair, b_iso, l_iso = pair("FeelIsolated")
        active_pair, b_active, l_active = pair("ActiveDaysPerWeek")

        improved_conf = bool(confidence_pair and l_conf > b_conf)
        improved_feeling = bool(feeling_pair and l_feel > b_feel)
        improved_movement = bool(movement_pair and l_move > b_move)
        less_isolated = bool(isolation_pair and l_iso < b_iso)
        more_active = bool(active_pair and l_active > b_active)

        outcome_rows.append({
            "Location": latest_row.Location,
            "Participants With Earlier Assessment": 1,
            "Confidence Paired": int(confidence_pair),
            "Improved Confidence To Join": int(improved_conf),
            "Feeling Confident Paired": int(feeling_pair),
            "Improved Feeling Confident": int(improved_feeling),
            "Movement Paired": int(movement_pair),
            "Improved Movement": int(improved_movement),
            "Isolation Paired": int(isolation_pair),
            "Less Isolated": int(less_isolated),
            "Active Days Paired": int(active_pair),
            "More Active Days": int(more_active),
            "Improved Fitness / Physical Activity": int(improved_movement or more_active),
        })

    if not outcome_rows:
        return pd.DataFrame()

    out = pd.DataFrame(outcome_rows).groupby("Location", as_index=False).sum()

    percent_pairs = [
        ("Improved Confidence To Join", "Confidence Paired", "% Improved Confidence To Join"),
        ("Improved Feeling Confident", "Feeling Confident Paired", "% Improved Feeling Confident"),
        ("Improved Movement", "Movement Paired", "% Improved Movement"),
        ("Less Isolated", "Isolation Paired", "% Less Isolated"),
        ("More Active Days", "Active Days Paired", "% More Active Days"),
    ]
    for numerator, denominator, output in percent_pairs:
        out[output] = np.where(
            out[denominator].eq(0),
            np.nan,
            (out[numerator] / out[denominator] * 100).round(1),
        )

    return out.sort_values("Location")


def data_quality(registrations):
    current = registrations[registrations["Period"] == "Current"].copy()

    checks = {
        "Missing DOB": current["DateOfBirth"].isna(),
        "Missing Postcode": current["Postcode"].fillna("").astype(str).str.strip().eq(""),
        "Missing Gender": current["Gender"].fillna("").astype(str).str.strip().eq(""),
        "Missing Ethnicity": current["Ethnicity"].fillna("").astype(str).str.strip().eq(""),
        "Missing Mobile": current["MobileNumber"].fillna("").astype(str).str.strip().eq(""),
        "Missing Site": current["Site"].fillna("").astype(str).str.strip().eq(""),
    }

    for name, mask in checks.items():
        current[name] = mask

    gap_cols = list(checks)
    current["Any Core Gap"] = current[gap_cols].any(axis=1)

    rows = []
    for location, g in current.groupby("Location"):
        row = {
            "Location": location,
            "New Registrations": int(len(g)),
            **{col: int(g[col].sum()) for col in gap_cols},
            "Participants With Any Core Gap": int(g["Any Core Gap"].sum()),
        }
        row["Data Gap %"] = (
            round(row["Participants With Any Core Gap"] / row["New Registrations"] * 100, 1)
            if row["New Registrations"] else 0.0
        )
        rows.append(row)

    return pd.DataFrame(rows).sort_values(
        ["Data Gap %", "New Registrations"], ascending=[False, False]
    )
