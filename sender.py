import smtplib
from email.mime.text import MIMEText

from pyspark.sql import DataFrame

from config import SETTINGS

# based on https://mailtrap.io/blog/python-send-email-gmail/


def format_email_body(detections: list[dict]) -> str:
    """
    Formats a list of detection dicts (from DETECTION_SCHEMA rows) into an email body.
    Groups alerts by cat_name for readability.
    """
    by_cat = {}
    for row in detections:
        by_cat.setdefault(row["cat_name"], []).append(row)

    lines = []
    for cat_name, alerts in by_cat.items():
        lines.append(f"{cat_name} ({len(alerts)} alert{'s' if len(alerts) > 1 else ''}):")
        for alert in alerts:
            detail = f"  - {alert['rule_triggered']}"
            if alert["details"]:
                detail += f": {alert['details']}"
            lines.append(detail)
        lines.append("")

    return "\n".join(lines).strip()


def send_detections(detections_df: DataFrame, password: str) -> None:
    """
    Takes a combined detections DataFrame (DETECTION_SCHEMA), formats it,
    and sends an email if there are any alerts.
    """
    rows = [row.asDict() for row in detections_df.collect()]
    if not rows:
        return

    sender = SETTINGS["email"]["sender"]
    recipient = SETTINGS["email"]["recipient"]
    cat_names = sorted({row["cat_name"] for row in rows})
    subject = f"Whisker alert: {', '.join(cat_names)} ({len(rows)} total)"
    body = format_email_body(rows)

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, [recipient], msg.as_string())
