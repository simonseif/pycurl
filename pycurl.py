#!/usr/bin/env python

import argparse
import io
import logging
import os
import threading
from functools import partial
from hashlib import md5
from queue import Queue
from typing import Iterable, Callable, TypeVar
from urllib.parse import urlparse

import requests
import urllib3
from urllib3.exceptions import HTTPError


def download(url: str, stream: io.IOBase, timeout: int, verify: bool):
    """
    Downloads the content of URL into stream.
    This method only supports HTTP and HTTPS URLs.
    The implementation is safe to use for large contents.
    :param url: URL to download.
    :param stream: stream to write content (e.g. file or I/O buffer).
    :param timeout: timeout until server sends data (not the overall download time).
    :param verify: verify server's SSL certificate.
    """
    with requests.get(url, timeout=timeout, verify=verify, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            stream.write(chunk)


def download_all(urls: Iterable[str], download_directory: str, downloader: Callable[[str, str], None]):
    """
    Downloads all URLs to download_directory.
    A URL that has been downloaded successfully will not be downloaded again.
    This method will continue if the download of a single URL fails for whatever reason.
    :param urls: URLs to download.
    :param download_directory directory to store the downloaded files. Will be created if it does not exist.
    :param downloader: callable that performs the actual download. The callable is expected to take the URL as
    first argument and write the content into the second argument.
    """
    os.makedirs(download_directory, exist_ok=True)
    for url in urls:
        # avoid headaches with URL symbols such as '/', '?', etc.
        filename = md5(bytearray(url, encoding='utf-8')).hexdigest()
        dst = os.path.join(download_directory, filename)
        try:
            with open(dst, 'xb') as f:  # O_EXCL|O_CREAT
                downloader(url, f)
                logging.info(f"downloaded '{url}' to '{dst}'")
        except FileExistsError:
            logging.info(f"'{url}' already (being) downloaded")
        except (requests.RequestException, HTTPError, IOError) as e:
            logging.error(f"error downloading '{url}': {e}")
            os.remove(dst)
        except Exception as e:
            # unexpected exception, probably a bug in this program
            logging.exception(e)
            os.remove(dst)


T = TypeVar('T')


def _dispatch(jobs: Iterable[T], consumer: Callable[[T], None], num_threads: int) -> None:
    """
    Dispatches items from a job queue to instances of a consumer.
    Each consumer is run in its own thread.
    The consumer is supposed to consume jobs from the queue sequentially until the queue is empty (StopIteration).
    It must not raise an exception in case it cannot process a job.

    :param jobs: an iterable of jobs.
    :param consumer: a callable that consumes and processes jobs.
    :param num_threads: number of threads to start.
    """
    queue = Queue(maxsize=num_threads)
    sentinel = object()  # marks end of jobs in queue

    workers = [threading.Thread(target=consumer, args=(iter(queue.get, sentinel),)) for _ in range(num_threads)]
    [w.start() for w in workers]

    for job in jobs:
        queue.put(job)

    [queue.put(sentinel) for _ in workers]
    [w.join() for w in workers]


def _read_urls(s: Iterable[str]) -> Iterable[str]:
    """
    Filters an iterator of strings for valid URLs.
    :param s: iterator of strings.
    :return: the strings in s that are valid URLs.
    """
    for i, line in enumerate(s):
        stripped_line = line.strip()
        if not stripped_line:
            continue  # just ignore empty lines
        if _is_valid_url(stripped_line):
            yield stripped_line
        else:
            logging.error(f"line {i + 1} is not a valid URL: '{stripped_line}'")


def _is_valid_url(s: str) -> bool:
    """
    Validates if string s is a URL.
    :param s: string to check
    :return: True if s is a valid URL. False otherwise.
    """
    (scheme, netloc, path, params, query, fragment) = urlparse(s)
    # valid URL contains at least a scheme and a host+port (netloc)
    return scheme and netloc


def _int_ge_1(v: str) -> int:
    """
    Helper type for argparse that represents integers greater or equal than 1.
    :param v: string.
    :return: parsed integer.
    :raises: ArgumentTypeError if v cannot be parsed as integer or an integer <1.
    """
    try:
        value = int(v)
    except ValueError:
        raise argparse.ArgumentTypeError(f'value needs to be integer greater or equal 1')
    if value < 1:
        raise argparse.ArgumentTypeError(f'value needs to be greater or equal 1')
    return value


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bulk download URLs')
    parser.add_argument('urls', help='path to the file containing the URLs')
    parser.add_argument('--download-dir', default='.', help='path to the download directory.'
                                                            ' defaults to the current working directory')
    parser.add_argument('--insecure', action='store_true', help='skip server SSL certificate validation')
    parser.add_argument('--parallelism', default=5, type=_int_ge_1, help='number of concurrent connections')
    parser.add_argument('--timeout', default=5, type=_int_ge_1,
                        help='timeout per request in seconds until the server starts to send data.')
    parser.add_argument('--verbose', action='store_true', help='increase verbosity of output')
    args = parser.parse_args()

    # if this module is imported elsewhere as library, consider injecting the logger as dependency
    if args.verbose:
        logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s:%(lineno)d]: %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='[%(asctime)s][%(levelname)s]: %(message)s', level=logging.INFO)

    # this feature is equally handy and insecure. maybe remove it?
    if args.insecure:
        # https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
        urllib3.disable_warnings()
    verify_ssl = not args.insecure

    url_reader = _read_urls(open(args.urls))

    downloader = partial(download, timeout=args.timeout, verify=verify_ssl)
    worker = partial(download_all, download_directory=args.download_dir, downloader=downloader)

    _dispatch(url_reader, worker, args.parallelism)
