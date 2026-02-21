import os
import sys
import json
from datetime import timedelta

# pip install faster-whisper ffmpeg-python
from faster_whisper import WhisperModel

def seconds_to_timestamp(s: float) -> str:
    td = timedelta(seconds=float(s))
    # Format like HH:MM:SS
    total_seconds = int(td.total_seconds())
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    sec = total_seconds % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"

def main():
    if len(sys.argv) < 2:
        print("Usage: python transcribe_faster_whisper.py <audio_file> [output_basename]")
        sys.exit(1)

    audio_path = sys.argv[1]
    out_base = sys.argv[2] if len(sys.argv) >= 3 else os.path.splitext(os.path.basename(audio_path))[0]

    if not os.path.exists(audio_path):
        print(f"File not found: {audio_path}")
        sys.exit(1)

    # Choose model size: tiny, base, small, medium, large-v3
    # small/medium is a good balance; large-v3 is best quality but slower.
    model_size = "small"

    # Device options:
    # - If you have NVIDIA GPU + CUDA: device="cuda", compute_type="float16"
    # - Otherwise CPU: device="cpu", compute_type="int8"
    device = "cpu"
    compute_type = "int8"

    print(f"Loading model '{model_size}' on {device} ({compute_type}) ...")
    model = WhisperModel(model_size, device=device, compute_type=compute_type)

    print("Transcribing... (this may take a while for long audio)")
    segments, info = model.transcribe(
        audio_path,
        beam_size=5,
        vad_filter=True,          # helps remove silence
        vad_parameters=dict(min_silence_duration_ms=500),
        word_timestamps=False
    )

    txt_path = f"{out_base}.txt"
    srt_path = f"{out_base}.srt"
    json_path = f"{out_base}.json"

    full_text_lines = []
    srt_lines = []
    json_segments = []

    idx = 1
    for seg in segments:
        start = seg.start
        end = seg.end
        text = seg.text.strip()

        full_text_lines.append(text)

        # SRT timestamp format: HH:MM:SS,mmm
        def srt_ts(sec):
            ms = int((sec - int(sec)) * 1000)
            hh = int(sec) // 3600
            mm = (int(sec) % 3600) // 60
            ss = int(sec) % 60
            return f"{hh:02d}:{mm:02d}:{ss:02d},{ms:03d}"

        srt_lines.append(str(idx))
        srt_lines.append(f"{srt_ts(start)} --> {srt_ts(end)}")
        srt_lines.append(text)
        srt_lines.append("")

        json_segments.append({
            "index": idx,
            "start_sec": start,
            "end_sec": end,
            "start": seconds_to_timestamp(start),
            "end": seconds_to_timestamp(end),
            "text": text
        })

        idx += 1

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(full_text_lines).strip() + "\n")

    with open(srt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(srt_lines).strip() + "\n")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "language": info.language,
                "language_probability": info.language_probability,
                "duration_sec": info.duration,
                "segments": json_segments
            },
            f,
            ensure_ascii=False,
            indent=2
        )

    print("\nDone ✅")
    print(f"Text:  {txt_path}")
    print(f"SRT:   {srt_path}")
    print(f"JSON:  {json_path}")
    print(f"Detected language: {info.language} (p={info.language_probability:.2f})")

if __name__ == "__main__":
    main()
