import csv
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import pyautogui


# =========================================================
# PATHS AND SETTINGS
# =========================================================

def resource_path(filename: str) -> Path:
    """
    Return the correct path for a normal Python script
    or a PyInstaller executable.
    """
    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / filename

    return Path(__file__).resolve().parent / filename


DOWNLOAD_BUTTON_IMAGE = resource_path("download_cv.png")
NEXT_BUTTON_IMAGE = resource_path("next_candidate.png")

DOWNLOADS_FOLDER = Path.home() / "Downloads"

OUTPUT_FOLDER = (
    Path.home()
    / "Documents"
    / "Saheli Recruitment"
    / "Health and Lifestyle Coordinator"
    / "CVs"
)

LOG_FILE = OUTPUT_FOLDER / "download_log.csv"

# Button-image matching level.
# Reduce to 0.75 if the screenshots are not detected.
IMAGE_CONFIDENCE = 0.80

# Maximum wait for each CV download.
DOWNLOAD_TIMEOUT_SECONDS = 90

# Wait after clicking the next-candidate arrow.
NEXT_CANDIDATE_LOAD_SECONDS = 3.0

ALLOWED_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".rtf",
    ".txt",
}

INCOMPLETE_EXTENSIONS = {
    ".crdownload",
    ".part",
    ".tmp",
}

# Move the mouse to the top-left corner to stop the script.
pyautogui.FAILSAFE = True

# General delay after PyAutoGUI actions.
pyautogui.PAUSE = 0.15


# =========================================================
# USER INPUT
# =========================================================

def ask_candidate_count() -> int:
    """Ask how many candidate CVs should be downloaded."""

    while True:
        value = input(
            "How many candidate CVs do you want to download? "
        ).strip()

        try:
            count = int(value)

            if count < 1:
                print("Please enter a number greater than 0.")
                continue

            if count > 500:
                print("Please enter a number between 1 and 500.")
                continue

            return count

        except ValueError:
            print(
                "Please enter a valid whole number, "
                "for example 2, 10 or 108."
            )


# =========================================================
# SETUP
# =========================================================

def validate_setup() -> None:
    """Confirm that the folders and button images exist."""

    if not DOWNLOADS_FOLDER.exists():
        raise FileNotFoundError(
            f"Downloads folder was not found:\n{DOWNLOADS_FOLDER}"
        )

    if not DOWNLOAD_BUTTON_IMAGE.exists():
        raise FileNotFoundError(
            "The Download CV screenshot was not found:\n"
            f"{DOWNLOAD_BUTTON_IMAGE}"
        )

    if not NEXT_BUTTON_IMAGE.exists():
        raise FileNotFoundError(
            "The next-candidate screenshot was not found:\n"
            f"{NEXT_BUTTON_IMAGE}"
        )

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


# =========================================================
# LOGGING
# =========================================================

def write_log(
    candidate_number: int,
    status: str,
    filename: str = "",
    message: str = "",
) -> None:
    """Record the result for each candidate in a CSV file."""

    log_exists = LOG_FILE.exists()

    with LOG_FILE.open(
        "a",
        newline="",
        encoding="utf-8-sig",
    ) as csv_file:
        writer = csv.writer(csv_file)

        if not log_exists:
            writer.writerow(
                [
                    "DateTime",
                    "CandidateNumber",
                    "Status",
                    "Filename",
                    "Message",
                ]
            )

        writer.writerow(
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                candidate_number,
                status,
                filename,
                message,
            ]
        )


# =========================================================
# CHROME DOWNLOAD POPUP
# =========================================================

def close_download_popup() -> None:
    """
    Close Chrome's Recent download history popup.

    Pressing Escape is safe when the popup is already closed.
    """

    pyautogui.press("esc")
    time.sleep(0.3)


# =========================================================
# DOWNLOAD FILE HANDLING
# =========================================================

def current_download_files() -> set[Path]:
    """Return completed files already present in Downloads."""

    return {
        file.resolve()
        for file in DOWNLOADS_FOLDER.iterdir()
        if file.is_file()
        and file.suffix.lower() not in INCOMPLETE_EXTENSIONS
    }


def unique_destination(filename: str) -> Path:
    """
    Create a unique destination path.

    Existing CV files will not be overwritten.
    """

    source_name = Path(filename)
    destination = OUTPUT_FOLDER / source_name.name
    counter = 2

    while destination.exists():
        destination = (
            OUTPUT_FOLDER
            / f"{source_name.stem} ({counter}){source_name.suffix}"
        )
        counter += 1

    return destination


def wait_for_new_download(
    files_before: set[Path],
) -> Path:
    """Wait until a new CV has completely downloaded."""

    started_at = time.time()

    while time.time() - started_at < DOWNLOAD_TIMEOUT_SECONDS:
        incomplete_downloads = [
            file
            for file in DOWNLOADS_FOLDER.iterdir()
            if file.is_file()
            and file.suffix.lower() in INCOMPLETE_EXTENSIONS
        ]

        completed_files = [
            file
            for file in DOWNLOADS_FOLDER.iterdir()
            if file.is_file()
            and file.resolve() not in files_before
            and file.suffix.lower() in ALLOWED_EXTENSIONS
        ]

        if completed_files and not incomplete_downloads:
            newest_file = max(
                completed_files,
                key=lambda item: item.stat().st_mtime,
            )

            # Confirm the file size has stopped changing.
            first_size = newest_file.stat().st_size
            time.sleep(1)
            second_size = newest_file.stat().st_size

            if first_size == second_size and second_size > 0:
                return newest_file

        time.sleep(0.5)

    raise TimeoutError(
        "No completed CV download was detected within "
        f"{DOWNLOAD_TIMEOUT_SECONDS} seconds."
    )


# =========================================================
# SCREEN IMAGE DETECTION
# =========================================================

def find_image(image_path: Path):
    """Find the centre position of a screenshot on the screen."""

    try:
        return pyautogui.locateCenterOnScreen(
            str(image_path),
            confidence=IMAGE_CONFIDENCE,
            grayscale=True,
        )

    except Exception as error:
        print(f"Image detection error: {error}")
        return None


def find_download_button():
    """
    Find the Download CV button quickly.

    Search order:
    1. Current screen.
    2. Jump directly to the bottom.
    3. Search upwards with Page Up.
    4. Search downwards from the top.
    """

    close_download_popup()

    # Check the current screen first.
    location = find_image(DOWNLOAD_BUTTON_IMAGE)

    if location:
        return location

    # Jump directly to the bottom.
    pyautogui.hotkey("ctrl", "end")
    time.sleep(0.7)

    location = find_image(DOWNLOAD_BUTTON_IMAGE)

    if location:
        return location

    # Search upwards quickly.
    for _ in range(8):
        pyautogui.press("pageup")
        time.sleep(0.25)

        location = find_image(DOWNLOAD_BUTTON_IMAGE)

        if location:
            return location

    # Search downwards from the top.
    pyautogui.hotkey("ctrl", "home")
    time.sleep(0.5)

    for _ in range(8):
        location = find_image(DOWNLOAD_BUTTON_IMAGE)

        if location:
            return location

        pyautogui.press("pagedown")
        time.sleep(0.25)

    return None


def find_next_button():
    """Return to the top and find the next-candidate arrow."""

    close_download_popup()

    pyautogui.hotkey("ctrl", "home")
    time.sleep(0.6)

    for _ in range(6):
        location = find_image(NEXT_BUTTON_IMAGE)

        if location:
            return location

        time.sleep(0.25)

    return None


# =========================================================
# CANDIDATE PROCESSING
# =========================================================

def process_candidate(
    candidate_number: int,
    total_candidates: int,
) -> bool:
    """Download the current candidate's CV."""

    print()
    print(
        f"[{candidate_number}/{total_candidates}] "
        "Searching for Download CV..."
    )

    close_download_popup()

    download_button = find_download_button()

    if download_button is None:
        message = "Download CV button was not found."

        print(f"FAILED: {message}")

        write_log(
            candidate_number=candidate_number,
            status="Failed",
            message=message,
        )

        return False

    files_before = current_download_files()

    pyautogui.click(download_button)

    print("Download clicked. Waiting for the CV...")

    try:
        downloaded_file = wait_for_new_download(files_before)

    except TimeoutError as error:
        close_download_popup()

        print(f"FAILED: {error}")

        write_log(
            candidate_number=candidate_number,
            status="Failed",
            message=str(error),
        )

        return False

    destination = unique_destination(downloaded_file.name)

    shutil.move(
        str(downloaded_file),
        str(destination),
    )

    print(f"Saved: {destination.name}")

    write_log(
        candidate_number=candidate_number,
        status="Downloaded",
        filename=destination.name,
    )

    # Close Chrome's Recent download history popup.
    close_download_popup()

    return True


def move_to_next_candidate() -> bool:
    """Close the download popup and open the next candidate."""

    close_download_popup()

    next_button = find_next_button()

    if next_button is None:
        print("Next-candidate arrow was not found.")
        return False

    pyautogui.click(next_button)

    print(
        "Opening the next candidate. "
        f"Waiting {NEXT_CANDIDATE_LOAD_SECONDS} seconds..."
    )

    time.sleep(NEXT_CANDIDATE_LOAD_SECONDS)

    close_download_popup()

    return True


# =========================================================
# MAIN PROGRAM
# =========================================================

def main() -> None:
    validate_setup()

    print("=" * 70)
    print("Indeed CV Downloader — Existing Chrome")
    print("=" * 70)
    print()

    total_candidates = ask_candidate_count()

    print()
    print("Before starting:")
    print("1. Open the first candidate in your normal Chrome.")
    print("2. Maximise the Chrome window.")
    print("3. Keep Chrome zoom at 100%.")
    print("4. Close the Chrome download popup if it is open.")
    print("5. Do not use the keyboard or mouse while it runs.")
    print()
    print("Emergency stop:")
    print("Move the mouse into the top-left corner of the screen.")
    print()
    print(f"Number of CVs selected: {total_candidates}")
    print()
    print(f"CV output folder:\n{OUTPUT_FOLDER}")
    print()

    input(
        "Press ENTER and immediately switch to the Chrome window: "
    )

    print()
    print("Starting in 7 seconds...")

    for seconds_remaining in range(7, 0, -1):
        print(seconds_remaining)
        time.sleep(1)

    successful = 0
    failed = 0

    for candidate_number in range(1, total_candidates + 1):
        try:
            success = process_candidate(
                candidate_number,
                total_candidates,
            )

            if success:
                successful += 1
            else:
                failed += 1

            if candidate_number >= total_candidates:
                break

            moved = move_to_next_candidate()

            if not moved:
                print(
                    "Stopping because the next-candidate "
                    "arrow could not be found."
                )
                break

        except pyautogui.FailSafeException:
            print()
            print("Emergency stop activated.")
            break

        except KeyboardInterrupt:
            print()
            print("Program stopped using Ctrl+C.")
            break

        except PermissionError as error:
            failed += 1

            print()
            print(
                "The downloaded file is open or locked by Chrome."
            )
            print(error)

            write_log(
                candidate_number=candidate_number,
                status="Error",
                message=str(error),
            )

            break

        except Exception as error:
            failed += 1

            close_download_popup()

            print()
            print(f"Unexpected error: {error}")

            write_log(
                candidate_number=candidate_number,
                status="Error",
                message=str(error),
            )

            break

    close_download_popup()

    print()
    print("=" * 70)
    print("Finished")
    print("=" * 70)
    print(f"Requested CVs: {total_candidates}")
    print(f"Successfully downloaded: {successful}")
    print(f"Failed or skipped: {failed}")
    print(f"CV folder: {OUTPUT_FOLDER}")
    print(f"Log file: {LOG_FILE}")
    print()

    input("Press ENTER to close this window...")


if __name__ == "__main__":
    try:
        main()

    except FileNotFoundError as error:
        print()
        print("SETUP ERROR")
        print(error)
        print()
        input("Press ENTER to close...")

    except Exception as error:
        print()
        print(f"PROGRAM ERROR: {error}")
        print()
        input("Press ENTER to close...")