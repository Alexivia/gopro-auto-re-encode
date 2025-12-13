"""
Custom GoPro re-encoding pipeline with metadata preservation and optional scuba color correction.

This script re-encodes GoPro videos using ffmpeg with the hevc_videotoolbox codec for faster
hardware-accelerated encoding on Apple Silicon Macs. It uses a constant quality target.
The ffmpeg commands and post-processing commands are designed such that metadata and other streams
are preserved in the final re-encoded file.

It can optionally run a scuba color correction step using an external script before re-encoding.
In this case, since the color correction is CPU-bound and single threaded, we can use asynchronous
programming to queue a configurable amount of color-corrected videos before starting the
re-encoding. Since the re-encoding uses hardware acceleration, only one at the time is scheduled.
The color corrected but not re-encoded file is not kept, as OpenCV uses very little compression
on its output, resulting in very large files.
"""

import argparse
import asyncio
import logging
import subprocess
from datetime import datetime
from pathlib import Path

UDTA_PATH = Path("./udtacopy/mac/udtacopy")
GPMF_PATH = Path("./gpmf-parser/gpmf-parser")
COLOR_CORRECT_SCRIPT = Path("./dive-color-corrector-correct.py")
DIVE_VENV_PATH = Path("./.venv-dive-color-corrector")
DIVE_VENV_PYTHON = DIVE_VENV_PATH / "bin" / "python"
LOGS_DIR = Path("./logs")

# Threading and concurrency settings
MAX_CONCURRENT_TASKS = 3
MAX_QUEUED_TASKS = 8


class StepFailedException(Exception):
    """Custom exception to indicate a step in the pipeline has failed.

    Carries an optional `returncode` to inspect the external process' exit code.
    """

    def __init__(self, message="Step failed.", returncode=None):
        super().__init__(message)
        self.returncode = returncode


def setup_logging(log_path):
    """Configure logging with both file and console handlers."""

    logger = logging.getLogger()
    # Ensure we capture DEBUG and above at the root logger so the file
    # handler can receive DEBUG messages while the console handler
    # will be limited to INFO.
    logger.setLevel(logging.DEBUG)

    # If setup_logging is called more than once, remove existing handlers
    # to avoid duplicated log lines.
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    # File handler: capture DEBUG and above to file
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler: only INFO and above should be printed to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def async_task_wrapper(task, *args, **kwargs):
    """Conditionally run async_task synchronously.

    If no event loop is running, run synchronously. Otherwise, schedule the async task.
    """

    try:
        _ = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop → run synchronously
        return asyncio.run(task(*args, **kwargs))
    else:
        # Already inside an event loop → schedule async
        return asyncio.create_task(task(*args, **kwargs))


def decode_std_out_err(stdout_b, stderr_b):
    """Safely decode bytes to str for logging and error messages."""

    try:
        stdout = stdout_b.decode()
    except (UnicodeDecodeError, AttributeError):
        stdout = repr(stdout_b)

    try:
        stderr = stderr_b.decode()
    except (UnicodeDecodeError, AttributeError):
        stderr = repr(stderr_b)

    return stdout, stderr


def process_subprocess_output(stdout_b, stderr_b, remove_control_chars=False):
    """Take a subprocess output in bytes, decode it, and optionally remove control characters.

    Replays the stream, removing content before the last carriage return on each line.
    This is useful for cleaning up ffmpeg/dive-color-corrector output that uses
    carriage returns to update progress in place (when running in a terminal window).
    """

    def replay_stream(stream_in):
        """Replay the stream, removing content before the last carriage return on each line."""

        stream_out_split = []
        stream_split = stream_in.split("\n")

        for line in stream_split:
            if line[-1] == "\r":
                line = line[:-1]

            if "\r" in line:
                pos = line.rfind("\r")
                if pos != -1:
                    line = line[pos + 1 :]

            stream_out_split.append(line)

        return "\n".join(stream_out_split).strip()

    stdout, stderr = decode_std_out_err(stdout_b, stderr_b)

    if remove_control_chars:
        stdout = replay_stream(stdout)
        stderr = replay_stream(stderr)

    return stdout, stderr


def run_udta(input_file, output_file):
    """Run udtacopy to copy GoPro metadata from input_file to output_file."""

    logging.info("Running udtacopy on %s -> %s", input_file, output_file)
    try:
        result = subprocess.run(
            [str(UDTA_PATH), input_file, output_file], check=False, capture_output=True
        )
        if result.returncode == 1:
            logging.info("UDTA exited with code 1, which is expected.")
        elif result.returncode != 0:
            logging.error("UDTA failed!")
            stdout, stderr = decode_std_out_err(result.stdout, result.stderr)
            logging.debug("UDTA stdout: %s", stdout)
            logging.debug("UDTA stderr: %s", stderr)
            raise StepFailedException("UDTA step failed.", returncode=result.returncode)
    except (FileNotFoundError, OSError) as e:
        logging.error("UDTA execution failed: %s", type(e).__name__)
        logging.debug("Error running UDTA: %s", e)
        raise StepFailedException("UDTA metadata copy failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("UDTA subprocess failed.")
        logging.debug("UDTA stderr: %s", e.stderr.decode() if e.stderr else repr(e))
        raise StepFailedException("UDTA metadata copy failed.") from e


def run_gpmf_parser(input_file):
    """Run gpmf-parser on input_file to validate GPMF stream.

    TODO: Currently not used in the pipeline, but may be useful for future
    validation steps.
    """

    logging.info("Running gpmf-parser on %s", input_file)
    try:
        subprocess.run([str(GPMF_PATH), input_file], check=True, capture_output=True)
    except (FileNotFoundError, OSError) as e:
        logging.error("gpmf-parser execution failed: %s", type(e).__name__)
        logging.debug("Error running gpmf-parser: %s", e)
        raise StepFailedException("gpmf-parser failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("gpmf-parser subprocess failed.")
        logging.debug(
            "gpmf-parser stderr: %s", e.stderr.decode() if e.stderr else repr(e)
        )
        raise StepFailedException("gpmf-parser failed.") from e


def run_exif(input_file, output_file):
    """Run exiftool to copy metadata from input_file to output_file."""

    logging.info(
        "Running exiftool to copy metadata from %s -> %s", input_file, output_file
    )
    try:
        subprocess.run(
            [
                "exiftool",
                "-overwrite_original_in_place",
                "-tagsFromFile",
                input_file,
                "-ee",
                "-all:all",
                output_file,
            ],
            check=True,
            capture_output=True,
        )
    except (FileNotFoundError, OSError) as e:
        logging.error("exiftool execution failed: %s", type(e).__name__)
        logging.debug("Error running exiftool: %s", e)
        raise StepFailedException("exiftool failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("exiftool subprocess failed.")
        logging.debug("exiftool stderr: %s", e.stderr.decode() if e.stderr else repr(e))
        raise StepFailedException("exiftool failed.") from e


async def dive_color_correction(input_file, output_file):
    """Run the dive-color-corrector script to color correct the input_file.

    This requires the dive-color-corrector virtual environment to be set up
    and the script to be present in the current directory.
    """

    if not DIVE_VENV_PATH.exists():
        logging.error("Dive color corrector: virtual environment not found.")
        logging.info(
            "Please create the virtual environment and install required packages first."
        )
        raise FileNotFoundError("Dive color corrector virtual environment not found.")

    if not COLOR_CORRECT_SCRIPT.exists():
        logging.error("Dive color corrector: script not found.")
        logging.info("Script %s not found in current directory.", COLOR_CORRECT_SCRIPT)
        raise FileNotFoundError("Dive color corrector script not found.")

    logging.info("Running dive-color-corrector on %s -> %s", input_file, output_file)
    try:
        # Call the virtualenv's python directly to avoid relying on shell builtins
        # like `source` which may not be present in the default /bin/sh used by
        # asyncio.create_subprocess_shell. Using create_subprocess_exec avoids a
        # shell and is safer for argument handling.
        cmd = [
            str(DIVE_VENV_PYTHON),
            str(COLOR_CORRECT_SCRIPT),
            "video",
            str(input_file),
            str(output_file),
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_b, stderr_b = await process.communicate()
        stdout, stderr = process_subprocess_output(
            stdout_b, stderr_b, remove_control_chars=True
        )

        if process.returncode != 0:
            logging.debug("dive-color-corrector stderr: %s", stderr)
            raise subprocess.CalledProcessError(
                process.returncode, cmd, output=stdout_b, stderr=stderr_b
            )
        logging.debug("dive-color-corrector output: %s", stdout)
    except (FileNotFoundError, OSError) as e:
        logging.error("dive-color-corrector execution failed: %s", type(e).__name__)
        logging.debug("Error running dive-color-corrector: %s", e)
        raise StepFailedException("dive-color-corrector failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("dive-color-corrector subprocess failed.")
        logging.debug(
            "dive-color-corrector stderr: %s",
            e.stderr.decode() if getattr(e, "stderr", None) else repr(e),
        )
        raise StepFailedException("dive-color-corrector failed.") from e

    logging.info("dive-color-corrector completed for %s.", input_file)


def ffprobe_wrapper(input_file):
    """Run ffprobe on input_file to validate its structure.

    TODO: Use this function in the pipeline for additional validation, including
    checking if a video has the audio channels or not. E.g., timelapses may not
    have audio and should be handled differently.
    """

    logging.info("Running ffprobe on %s", input_file)
    try:
        subprocess.run(
            ["ffprobe", "-hide_banner", input_file], check=True, capture_output=True
        )
        subprocess.run(
            ["ffprobe", "-hide_banner", "-i", input_file, "-show_streams"],
            check=True,
            capture_output=True,
        )
    except (FileNotFoundError, OSError) as e:
        logging.error("ffprobe execution failed: %s", type(e).__name__)
        logging.debug("Error running ffprobe: %s", e)
        raise StepFailedException("ffprobe failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("ffprobe subprocess failed.")
        logging.debug("ffprobe stderr: %s", e.stderr.decode() if e.stderr else repr(e))
        raise StepFailedException("ffprobe failed.") from e


def gopro_ffmpeg_command(input_file, output_file, quality, scuba_file=None):
    """
    In case scuba_file is provided, then this will hold the color corrected
    video file. Provide both the original video file (holding the audio and
    metadata streams) and the new color corrected video file (holding only the
    video stream), and select the appropriate streams in the de-multiplexing and
    encoding command.

    This will only copy the GPMD (GoPro MET) data stream, and drop the TMCD
    (GoPro TCD) and FDSC (GoPro SOS) streams, as ffmpeg has difficulties
    parsing them.
    """

    ffmpeg_cmd = []
    ffmpeg_cmd += [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-i",
        input_file,
    ]

    if not scuba_file:
        logging.info(
            'Re-encoding "%s" to "%s" with quality %s.',
            input_file,
            output_file,
            quality,
        )
        ffmpeg_cmd += [
            "-map_metadata",
            "0",
            "-copy_unknown",
            "-map",
            "0:v",
        ]
    else:
        logging.info(
            'Re-encoding "%s" (original) + "%s" (scuba corrected) to "%s" with quality %s.',
            input_file,
            scuba_file,
            output_file,
            quality,
        )
        ffmpeg_cmd += [
            "-i",
            scuba_file,
            "-map_metadata",
            "0",
            "-copy_unknown",
            "-map",
            "1:v",
        ]

    ffmpeg_cmd += [
        "-c:v",
        "hevc_videotoolbox",
        "-q:v",
        str(quality),
        "-preset",
        "slow",
        "-tag:v",
        "hvc1",
        "-map",
        "0:a",
        "-c:a",
        "copy",
        "-map",
        "0:d:1",
        "-c:d:1",
        "copy",
        "-f",
        "mp4",
        output_file,
    ]

    return ffmpeg_cmd


async def gopro_reencode(input_file, output_file, quality, scuba_file=None):
    """
    Re-encode a GoPro video, maintaining metadata and other streams.

    Optionally, multiplexes in a scuba color corrected video stream.
    """

    logging.info("Verifying input file structure for %s", input_file)
    try:
        process = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-hide_banner",
            "-i",
            input_file,
            "-show_streams",
            "-of",
            "json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_b, stderr_b = await process.communicate()
        stdout, stderr = process_subprocess_output(stdout_b, stderr_b)

        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode, "ffprobe", output=stdout_b, stderr=stderr_b
            )
        if "error" in stderr.lower():
            logging.error("Input file structure verification failed.")
            logging.debug("ffprobe stderr: %s", stderr)
            raise StepFailedException("ffprobe step failed due to input file error.")
    except (FileNotFoundError, OSError) as e:
        logging.error("ffprobe execution failed: %s", type(e).__name__)
        logging.debug("ffprobe error: %s", e)
        raise StepFailedException("Input file analysis failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("ffprobe subprocess failed.")
        logging.debug("ffprobe stderr: %s", stderr)
        raise StepFailedException("Input file analysis failed.") from e

    # Get the ffmpeg command, in list format.
    ffmpeg_cmd = gopro_ffmpeg_command(
        input_file, output_file, quality, scuba_file=scuba_file
    )

    logging.info("Running ffmpeg for %s", output_file)
    try:
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_b, stderr_b = await process.communicate()
        stdout, stderr = process_subprocess_output(stdout_b, stderr_b)

        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode, ffmpeg_cmd, output=stdout_b, stderr=stderr_b
            )
        logging.debug("ffmpeg output: %s", stdout)
        logging.debug("ffmpeg error output: %s", stderr)
    except (FileNotFoundError, OSError) as e:
        logging.error("ffmpeg execution failed: %s", type(e).__name__)
        logging.debug("Error running ffmpeg: %s", e)
        raise StepFailedException("Re-encoding failed.") from e
    except subprocess.CalledProcessError as e:
        logging.error("ffmpeg subprocess failed.")
        logging.debug(
            "ffmpeg stderr: %s",
            e.stderr.decode() if getattr(e, "stderr", None) else repr(e),
        )
        raise StepFailedException("Re-encoding failed.") from e

    logging.info("Post-processing metadata for %s.", output_file)
    try:
        run_udta(input_file, output_file)
        run_exif(input_file, output_file)
    except StepFailedException:
        logging.error("Post-processing steps failed due to a known pipeline error.")
        raise
    except Exception as e:
        logging.error("Post-processing steps failed.")
        logging.debug("Error in post-processing: %s", e)
        raise StepFailedException(
            "Post-processing the re-encoded file metadata failed."
        ) from e

    logging.info("Re-encoding completed for %s.", input_file)


def encode_routine(input_files, output_directory, quality, is_scuba):
    """Encode routine for a list of input files.

    Quality is the constant quality factor for hevc_videotoolbox encoding.
    If is_scuba is True, runs the scuba color correction step before re-encoding.
    """

    # Collect failed files so the pipeline can continue processing others
    # and report failures at the end.
    failed_files = []

    if is_scuba:
        logging.info("Starting encoding routine for %d scuba files.", len(input_files))
        logging.info("This is an asynchronous concurrent routine.")
        logging.info(
            "At most %d color correction tasks will run simultaneously.",
            MAX_CONCURRENT_TASKS,
        )
        logging.info(
            "At most %d color correction tasks will be queued, waiting for being re-encoded.",
            MAX_QUEUED_TASKS,
        )
        logging.info("----------------------------------------")
        logging.info("")

        asyncio.run(
            _encode_routine_scuba_async(
                input_files, output_directory, quality, failed_files
            )
        )
    else:
        logging.info("Starting encoding routine for %d files.", len(input_files))
        logging.info("----------------------------------------")
        logging.info("")

        for input_file in input_files:
            output_file = output_directory / input_file.name

            try:
                async_task_wrapper(
                    gopro_reencode, str(input_file), str(output_file), quality
                )
            except StepFailedException as e:
                # StepFailedException is raised by gopro_reencode on any failure.
                logging.error("Re-encoding failed for file: %s, skipping.", input_file)
                logging.debug("Error in re-encoding (pipeline): %s", e)
                failed_files.append((str(input_file), repr(e)))

    # Return list of failures for reporting by caller.
    return failed_files


async def _encode_routine_scuba_async(
    input_files, output_directory, quality, failed_files
):
    concurrency_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    # A queue that holds ready-to-re-encode items. We also keep a separate
    # semaphore (max_queueing_semaphore) that limits how many color-corrected
    # files may exist on disk / be waiting in the pipeline. Producers must
    # acquire a slot before they begin color correction, and the consumer
    # releases the slot when it consumes the queued item. This prevents
    # unbounded creation of temporary scuba files when many producers finish
    # while the consumer is slower.
    color_corrected_queue = asyncio.Queue(MAX_QUEUED_TASKS)
    max_queueing_semaphore = asyncio.Semaphore(MAX_QUEUED_TASKS)
    ready_color_corrected_count = 0
    all_color_correction_done = asyncio.Event()

    async def run_dive_color_correction(input_file, output_file, temp_scuba_file):
        # Reserve a slot for this color-corrected file before starting work.
        # This prevents creating more than MAX_QUEUED_TASKS temp files on disk.
        await max_queueing_semaphore.acquire()
        error = None
        try:
            async with concurrency_semaphore:
                try:
                    await dive_color_correction(str(input_file), str(temp_scuba_file))
                except StepFailedException as e:
                    # dive_color_correction raises StepFailedException on any failure.
                    logging.error("Color correction failed for file: %s", input_file)
                    logging.debug("Error in color correction: %s", e)
                    error = e

            # Put exactly one record into the queue describing the result.
            # Because we acquired a queue slot, this put should not block.
            await color_corrected_queue.put(
                (input_file, output_file, temp_scuba_file, error)
            )
        except Exception:
            # In case putting into the queue fails for any reason, make sure we
            # release the reserved slot to avoid leaking reservations.
            try:
                max_queueing_semaphore.release()
            except Exception:
                logging.debug("Failed to release queue slot reservation after error.")
            raise

    async def run_gopro_reencode():
        nonlocal ready_color_corrected_count

        while True:
            if (
                ready_color_corrected_count >= len(input_files)
                and all_color_correction_done.is_set()
            ):
                break

            input_file, output_file, temp_scuba_file, error = (
                await color_corrected_queue.get()
            )

            if not error:
                logging.info("File ready for re-encoding: %s", input_file)
                try:
                    await gopro_reencode(
                        str(input_file),
                        str(output_file),
                        quality,
                        scuba_file=str(temp_scuba_file),
                    )
                except StepFailedException as e:
                    logging.error("Re-encoding failed for file: %s", input_file)
                    logging.debug("Error in re-encoding: %s", e)
                    failed_files.append((str(input_file), repr(e)))
            else:
                logging.info(
                    "Skipping re-encoding for file %s due to prior color correction error.",
                    input_file,
                )
                # Record the prior color-correction error as a failure for reporting.
                try:
                    failed_files.append((str(input_file), repr(error)))
                except (RuntimeError, ValueError):
                    # Best-effort only: don't crash the pipeline if logging fails here.
                    logging.debug("Failed to append failure for %s", input_file)

            # Clean up temporary scuba file after a re-encoding attempt,
            # whether it succeeded or failed. If the temp file exists, remove it.
            if temp_scuba_file.exists():
                try:
                    logging.info(
                        "Removing temporary color correction file: %s", temp_scuba_file
                    )
                    temp_scuba_file.unlink()
                except OSError as e_inner:
                    logging.warning(
                        "Could not remove temporary scuba file %s.", temp_scuba_file
                    )
                    logging.debug("Error while removing temp scuba file: %s", e_inner)

            ready_color_corrected_count += 1
            # Mark queue item done. This indicates we have consumed the item.
            color_corrected_queue.task_done()
            try:
                # Release the reservation so another producer may start color correction.
                max_queueing_semaphore.release()
            except Exception:
                logging.debug("Failed to release queue slot reservation.")

    task_gopro_reencode = asyncio.create_task(run_gopro_reencode())
    tasks_dive_color_correction = []

    for input_file in input_files:
        output_file = output_directory / input_file.name
        temp_scuba_file = output_directory / f"scuba_temp_{input_file.stem}.mp4"

        task = asyncio.create_task(
            run_dive_color_correction(input_file, output_file, temp_scuba_file)
        )
        tasks_dive_color_correction.append(task)

    await asyncio.gather(*tasks_dive_color_correction)
    all_color_correction_done.set()
    await color_corrected_queue.join()
    await task_gopro_reencode


def main():
    """Main."""

    parser = argparse.ArgumentParser(description="Custom GoPro re-encoding pipeline")
    parser.add_argument(
        "mode",
        choices=["file", "list"],
        help="Mode: file for single file, list for file list",
    )
    parser.add_argument("input", help="Input file or file list")
    parser.add_argument("output", help="Output directory")
    parser.add_argument(
        "-q",
        "--quality",
        dest="quality",
        type=int,
        default=30,
        help="HEVC videotoolbox constant quality factor: 1-100, higher is better",
    )
    parser.add_argument(
        "-s",
        "--scuba",
        dest="is_scuba",
        action="store_true",
        help="Indicates if the video is scuba footage, running extra pipeline to color grade the video before re-encoding.",
    )
    args = parser.parse_args()

    LOGS_DIR.mkdir(exist_ok=True)
    log_path = (
        LOGS_DIR / f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}-gopro-reenc.log"
    )
    setup_logging(log_path)

    # Normalize and resolve the output directory early.
    output_directory = Path(args.output).expanduser().resolve()
    if not output_directory.exists():
        logging.info("Creating output directory: %s", output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)

    files_to_parse = []

    # Handle single-file mode and list-file mode with robust path resolution.
    if args.mode == "file":
        input_path = Path(args.input).expanduser()
        # If a relative path was provided, resolve against current working dir.
        if not input_path.is_absolute():
            input_path = (Path.cwd() / input_path).resolve()
        if not input_path.is_file():
            logging.error("Input file not found: %s", input_path)
            raise FileNotFoundError(f"Input file not found: {input_path}")
        files_to_parse.append(input_path)
    elif args.mode == "list":
        list_file = Path(args.input).expanduser()
        if not list_file.is_file():
            raise FileNotFoundError(f"Input file list not found: {list_file}")
        # Interpret relative paths inside the list as relative to the list file's directory.
        with open(list_file, "r", encoding="utf-8") as f:
            for line in f:
                entry = line.strip()
                if not entry:
                    continue
                p = Path(entry).expanduser()
                if not p.is_absolute():
                    p = (list_file.parent / p).resolve()
                if not p.is_file():
                    logging.error("Listed input file not found, skipping: %s", p)
                    continue
                files_to_parse.append(p)
    else:
        logging.error("Invalid mode: %s", args.mode)
        raise ValueError(f"Invalid mode: {args.mode}")

    failures = encode_routine(
        files_to_parse, output_directory, args.quality, args.is_scuba
    )

    if failures:
        logging.error("Re-encoding completed with %d failures:", len(failures))
        for f, reason in failures:
            logging.error(" - %s : %s", f, reason)
    else:
        logging.info("All done! No failures reported.")


if __name__ == "__main__":
    main()
