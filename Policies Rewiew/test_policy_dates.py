from pathlib import Path
from docx import Document
import re

POLICY_FOLDER = Path(
    r"C:\Users\shonk\Saheli Hub\Saheli Hub - Policies"
)

# We only print lines that look like document-control metadata.
# Policy body text is NOT printed.
KEYWORDS = [
    "review",
    "approved",
    "approval",
    "version",
    "effective",
    "issue date",
    "issued",
    "policy owner",
    "document owner",
    "author",
    "date created",
    "last updated",
    "next review",
]


def clean(text):
    return re.sub(r"\s+", " ", str(text)).strip()


def is_metadata(text):
    lower = text.lower()

    return any(
        keyword in lower
        for keyword in KEYWORDS
    )


def inspect_docx(path):
    results = []

    try:
        doc = Document(str(path))

        # Paragraphs
        for paragraph in doc.paragraphs:
            text = clean(paragraph.text)

            if text and is_metadata(text):
                results.append(
                    ("PARAGRAPH", text[:250])
                )

        # Tables - very important because policy control
        # information is often stored here.
        for table_number, table in enumerate(
            doc.tables,
            start=1
        ):
            for row_number, row in enumerate(
                table.rows,
                start=1
            ):
                cells = [
                    clean(cell.text)
                    for cell in row.cells
                ]

                row_text = " | ".join(
                    cell for cell in cells if cell
                )

                if row_text and is_metadata(row_text):
                    results.append(
                        (
                            f"TABLE {table_number} ROW {row_number}",
                            row_text[:300]
                        )
                    )

        return results, None

    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


print("=" * 100)
print("SAHELI POLICY METADATA TEST")
print("=" * 100)

files = sorted(
    POLICY_FOLDER.rglob("*.docx")
)

success = 0
failed = 0
metadata_found = 0

for path in files:

    results, error = inspect_docx(path)

    print()
    print("-" * 100)
    print(path.name)
    print("-" * 100)

    if error:
        print("ERROR:", error)
        failed += 1
        continue

    success += 1

    if not results:
        print("No obvious review/version metadata found.")
        continue

    metadata_found += 1

    # Limit output so we don't dump large amounts
    # of policy content accidentally.
    for source, text in results[:10]:
        print(f"{source}: {text}")

    if len(results) > 10:
        print(
            f"... {len(results) - 10} additional "
            f"metadata-looking lines not displayed."
        )


print()
print("=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Files checked:          {len(files)}")
print(f"Successfully opened:    {success}")
print(f"Failed to open:         {failed}")
print(f"Metadata found in:      {metadata_found}")
print(f"No metadata detected:   {success - metadata_found}")
print()
print("No files were changed.")