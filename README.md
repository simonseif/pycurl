# pycurl

Bulk URL downloader.

## Requirements

See `Pipfile`

## Usage

```
usage: pycurl.py [-h] [--download-dir DOWNLOAD_DIR] [--insecure]
                 [--parallelism PARALLELISM] [--timeout TIMEOUT] [--verbose]
                 urls

Bulk download URLs

positional arguments:
  urls                  path to the file containing the URLs

optional arguments:
  -h, --help            show this help message and exit
  --download-dir DOWNLOAD_DIR
                        path to the download directory. defaults to the current
                        working directory
  --insecure            skip server SSL certificate validation
  --parallelism PARALLELISM
                        number of concurrent connections
  --timeout TIMEOUT     timeout per request in seconds until the server starts
                        to send data.
  --verbose             increase verbosity of output
```

### Example
`./pycurl.py --download-dir=nasa examples/nasa-pictures`


### Run Tests
`python -m unittest discover`

## Limitations:
- no support to limit the bandwidth
- no support to rate limit requests per host
- available disk space is not considered (this script might fill up your disk)
- no support for transmission timeout (time for an entire download)
- HTTP's persistent connection feature is not used
- limited to HTTP and HTTPS
- SIGTERM is not handled properly
- logging might get messy if `--verbose` flag is passed (requests lib logs excessively)
- proxy support is untested
- unit tests are not completely silent
- unit tests for insecure https connections rely on external resources (badssl.com).
- unit tests trigger a `ResourceWarning: unclosed <socket.socket>` caused by the requests library.
  See https://github.com/psf/requests/issues/1882#issuecomment-52282635
- duplicate URL detection is based on posix file system feature. See http://man7.org/linux/man-pages/man2/open.2.html O_EXCL for
  limitations. Unclear if this works reliably on Windows.