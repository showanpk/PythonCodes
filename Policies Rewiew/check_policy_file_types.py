from pathlib import Path
import zipfile

POLICY_FOLDER = Path(
    r"C:\Users\shonk\Saheli Hub\Saheli Hub - Policies"
)

print("=" * 80)
print("SAHELI POLICY FILE TYPE CHECK")
print("=" * 80)

files = list(POLICY_FOLDER.rglob("*.docx"))

print(f"\nDOCX files found: {len(files)}\n")

valid = 0
invalid = 0

for path in files:

    try:
        size = path.stat().st_size

        # Read only first few bytes to identify format
        with open(path, "rb") as f:
            header = f.read(16)

        if zipfile.is_zipfile(path):
            status = "VALID DOCX"
            valid += 1

        elif header.startswith(b"\xd0\xcf\x11\xe0"):
            status = "OLD WORD FORMAT (.doc renamed as .docx)"
            invalid += 1

        elif header.startswith(b"PK"):
            status = "ZIP/OFFICE FILE - NEEDS CHECK"
            invalid += 1

        elif header.startswith(b"%PDF"):
            status = "PDF RENAMED AS DOCX"
            invalid += 1

        elif header.startswith(b"<"):
            status = "XML/HTML TYPE FILE"
            invalid += 1

        else:
            status = "UNKNOWN / POSSIBLY PLACEHOLDER"
            invalid += 1

        print(
            f"{status:42} | "
            f"{size / 1024:9.1f} KB | "
            f"{path.name}"
        )

    except Exception as e:
        print(
            f"ERROR                                      | "
            f"{path.name} | {type(e).__name__}"
        )


print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print(f"Valid DOCX:   {valid}")
print(f"Other/Issue:  {invalid}")
print(f"Total:        {len(files)}")

print("\nNo policy text was printed or exported.")
print("No files were modified.")