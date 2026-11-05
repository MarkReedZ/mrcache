# Upstream io_uring sources

These files were downloaded so this tree has a local copy of the upstream
references. `docs/io_uring.md` is the project note written for people and
coding agents working on mrcache. It is not a transcription of either PDF.

| File | Source | What it is |
| --- | --- | --- |
| `efficient-io-with-io_uring.pdf` | https://kernel.dk/io_uring.pdf | Jens Axboe, *Efficient IO with io_uring*. The original design paper: why aio was insufficient, the shared submission and completion rings, `io_uring_setup` / `io_uring_enter` / `io_uring_register`, polling, and fixed buffers. Written against the 5.1-era interface. |
| `io_uring-whats-new.pdf` | https://kernel.dk/io_uring-whatsnew.pdf | Short follow-up on linked SQEs, completion ordering, and fixed files and buffers. |

Later features that mrcache already uses (buffer rings, multishot accept and
recv, `SINGLE_ISSUER`) post-date those papers. The rules for them are in
`docs/io_uring.md`, checked against the uapi header `linux/io_uring.h` and
against Jens Axboe's *io_uring and networking in 2023* note:

https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023

liburing man pages (`io_uring_setup(2)`, `io_uring_enter(2)`,
`io_uring_prep_read(3)`, `io_uring_prep_write_fixed(3)`,
`io_uring_register_buffers(3)`, `io_uring_register_files(3)`,
`io_uring_register_buf_ring(3)`) are the contract for individual calls.
Kernel headers in this environment identify the opcode and flag names.
