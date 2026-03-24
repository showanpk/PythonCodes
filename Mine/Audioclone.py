from pathlib import Path

import torch
from TTS.api import TTS


MODEL_NAME = "tts_models/multilingual/multi-dataset/xtts_v2"
TEXT = "Atchara Muthu"
LANGUAGE = "en"
SPEAKER_WAV = Path(r"C:\Users\shonk\AppData\Local\CapCut\Videos\0321 (1).WAV")
OUTPUT_WAV = Path(r"C:\Users\shonk\source\PythonCodes\Mine\output.wav")


def main() -> None:
    if not SPEAKER_WAV.exists():
        raise FileNotFoundError(f"Speaker WAV not found: {SPEAKER_WAV}")

    OUTPUT_WAV.parent.mkdir(parents=True, exist_ok=True)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Loading model on: {device}")
    tts = TTS(MODEL_NAME).to(device)

    print("Generating cloned voice...")
    tts.tts_to_file(
        text=TEXT,
        speaker_wav=str(SPEAKER_WAV),
        language=LANGUAGE,
        file_path=str(OUTPUT_WAV),
    )
    print(f"Done. Output saved to: {OUTPUT_WAV}")


if __name__ == "__main__":
    main()