# GoPro Auto Re-encode

A tool for automatically re-encoding (a list of) GoPro files.

## Project Description

This script automates the process of re-encoding GoPro videos while preserving metadata tracks, optionally color-correcting scuba videos using an external tool, and more.

### Features

- When re-encoding and color correcting multiple files (in list mode), the script spawns multiple color correction processes in parallel.
    - The color correction process is single threaded, so having multiple processes running concurrently speeds up the overall processing time.
    - The number of parallel processes can be configured via a script constant.
    - The script makes sure only a limited number of color corrected files are ready to be transcoded at any given time, since the transcoding step is not able to consume files faster than the color correction step produces them.
    - The transcoding is dispatched to the hardware encoder, so it is performed sequentially - whatever color corrected file is ready first is transcoded first.

## Pre-requisites

- `ffmpeg` (with `hevc_videotoolbox` support)
    - I use an M1 Mac, so I take advantage of the hardware encoder/decoder.
    - Install via [Homebrew](https://brew.sh/).
- `exiftool`
    - Also installed via Homebrew.
- [`udtacopy`](https://github.com/gopro/labs/tree/master/docs/control/chapters/bin)
    - A tool to copy GoPro metadata streams from one container to another without re-encoding.
    - Download and unzip the `.zip` to the project directory
- Python 3.x
- (Optional) [Dive Color Corrector](https://github.com/bornfree/dive-color-corrector)
    - A tool to color-correct underwater videos.
    - Create a Python virtual environment for isolated dependencies

## GoPro stream names

- **GoPro AVC** () / **GoPro H.265** (hvc1)
- **GoPro AAC** (mp4a)
    - Not present in time-lapse videos.
- **GoPro TCD** (tmcd)
- **GoPro MET** (gpmd)
- **GoPro SOS** (fdsc)
    - `ffmpeg` does not support this stream, and for some reason it is not copied **even** over with `-copy_unknown`.

## Future Features (TODOs)

### Support time-lapse videos

Time-lapse videos do not have an audio stream, so the current script fails when trying to copy the audio stream.

We need to add logic to check if the audio stream exists before trying to copy it.

### Concatenate GoPro videos

To concatenate separate GoPro videos into a single file, while preserving the other data streams on the `.mp4` container, create a list of all the file paths corresponding to a single movie into a `.txt` file. Use this `.txt` file to feed the file names into `ffmpeg`.

The contents of the file should be a line per file:
- `file /path/to/gopro/file.MP4`

```bash
ffmpeg -f concat -safe 0 -i <filname list>.txt \
       -map_metadata 0 -c copy \
       -map 0:v -map 0:a -map 0:3 \
       -tag:2 gpmd \
       -copy_unknown \
       <output file>.MP4

touch -r <input file>.MP4 <output file>.MP4

exiftool -tagsFromFile <input file>.MP4 -All:All <output file>.MP4
```

### Extract GoPro streams to GPX

There exists a Python package that helpfully fetches the GPS stream from the video file and stores it into a separate GPX file.

[gopro2gpx](https://github.com/juanmcasillas/gopro2gpx)

This package takes a GoPro `.mp4` as input and parses the `gpmd` stream inside
it to extract GPX information from the camera's GPS receiver.

```bash
source ~/Movies/GoPro/pyenv_gpx/bin/activate

gopro2gpx -s -vv <gopro file>.MP4 <output file names>
```

As an addition, the command also saves the remaining binary data (MEMS sensors,
etc.) into a `.bin` file.

To visualize simple GPX reports: https://www.trackreport.net


### Lower resolution

```bash
caffeinate -d ffmpeg -ss <start time HH:MM:SS.xxx> -i <input> -vf scale=-2:1080 -c:v libx265 -crf 28 -preset fast -x265-params "vbv-maxrate=15000:vbv-bufsize=20000" -c:a copy -c:2 copy -tag:2 gpmd -copy_unknown -to <stop time HH:MM:SS.xxx> <output>

```

# Credits

This project took a lot of experimenting since I am not the most avid `ffmpeg` connaisseur. But, thanks to the help of the bellow projects/blogs I was able to pull something through. Thank you so much for your work and contributions :pray:

## GitHub Projects

- [bornfree/dive-color-corrector](https://github.com/bornfree/dive-color-corrector)
- [rubegartor/ReelSteady-Joiner/](https://github.com/rubegartor/ReelSteady-Joiner/)
- [mifi/lossless-cut](https://github.com/mifi/lossless-cut)
- [gopro/gpmf-parser](https://github.com/gopro/gpmf-parser)
- [gopro/labs](https://github.com/gopro/labs)

## Blog Posts / Forum Threads

- https://brandur.org/fragments/ffmpeg-h265
- https://coderunner.io/how-to-compress-gopro-movies-and-keep-metadata/
- https://exiftool.org/forum/index.php?topic=10405.0
- https://trac.ffmpeg.org/ticket/8338

# Other References

- https://trac.ffmpeg.org/wiki/HWAccelIntro#VideoToolbox
